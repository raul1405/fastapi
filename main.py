# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import unicodedata
import traceback
import re
import time
import threading

from bs4 import BeautifulSoup  # already in your worker env

app = FastAPI()

# ---------------- Config ----------------
INDEX_TTL_SECONDS = 600   # 10 minutes cache TTL; tune as needed
REBUILD_TIME_BUDGET = 25  # seconds; stop early if taking too long

# --------------- Models -----------------
class SearchIn(BaseModel):
    username: str
    password: str
    q: str = ""
    limit: Optional[int] = 20

class ReindexIn(BaseModel):
    username: str
    password: str
    pp_ids: Optional[List[str]] = None  # reserved; not used in current builder

# --------------- Cache ------------------
# CACHE: {
#   username: {
#       "items": List[dict],
#       "updated": float,
#       "building": bool,
#       "last_error": str|None,
#       "build_started": float|None,
#       "build_finished": float|None
#   }
# }
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()

def _now() -> float:
    return time.time()

def _is_fresh(entry: Dict[str, Any]) -> bool:
    return entry and ((_now() - entry.get("updated", 0.0)) < INDEX_TTL_SECONDS)

# -------------- Text utils --------------
def _norm(s: str) -> str:
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _split_lecturers(prof: str) -> List[str]:
    if not prof:
        return []
    parts = re.split(r"[·•|,;/]+", prof)
    return [p.strip() for p in parts if p and p.strip()]

def _matches(tokens: List[str], title: str, prof: str, lv_id: str) -> bool:
    if not tokens:
        return True
    hay = " ".join(filter(None, [title, prof, str(lv_id)]))
    hay_n = _norm(hay)
    return all(t in hay_n for t in tokens)

# -------------- LPIS client -------------
def get_lpis_client(user: str, pw: str):
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")
    try:
        return WuLpisApi(user, pw, args=None, sessiondir=None)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

# --------- Low-level scraping (fast) ----
def _parse_lv_rows_fast(pp_id: str, soup_lv: BeautifulSoup, tokens: List[str], cap: Optional[int], out: List[Dict[str, Any]]) -> bool:
    """
    Parse a PP's LV table quickly and append only matching rows to 'out'.
    Returns True if cap reached and caller can stop.
    """
    lv_table = soup_lv.find("table", {"class": "b3k-data"})
    lv_body = lv_table.find("tbody") if lv_table else None
    if not lv_body:
        return False

    for row in lv_body.find_all("tr"):
        ver_id_link = row.select_one(".ver_id a")
        if not ver_id_link:
            continue
        lv_id = (ver_id_link.get_text(" ", strip=True) or "").strip()
        if not lv_id:
            continue

        # semester
        sem_span = row.select_one(".ver_id span")
        semester = (sem_span.get_text(" ", strip=True) if sem_span else None)

        # lecturer(s)
        prof_div = row.select_one(".ver_title div")
        prof = (prof_div.get_text(" ", strip=True) if prof_div else "").strip()

        # title (robust)
        name_td = row.find("td", {"class": "ver_title"})
        title = ""
        if name_td:
            # Prefer an inner title element if present
            title_el = name_td.select_one("a, strong, span")
            title = (title_el.get_text(" ", strip=True) if title_el else name_td.get_text(" ", strip=True)).strip()
            if prof:
                title = re.sub(re.escape(prof) + r"\s*$", "", title).strip(" -·•\u00A0")
            title = re.sub(r"\s+", " ", title).strip()

        # fast matching (AND over tokens on title+prof+lv_id)
        if tokens:
            hay = " ".join(filter(None, [title, prof, lv_id]))
            hay_n = _norm(hay)
            if not all(t in hay_n for t in tokens):
                continue

        # status
        status_div = row.select_one("td.box div")
        status = (status_div.get_text(" ", strip=True) if status_div else None)

        # capacity/free
        cap_div = row.select_one('div[class*="capacity_entry"]')
        free_val, cap_val = (None, None)
        if cap_div:
            cap_txt = (cap_div.get_text(" ", strip=True) or "")
            try:
                slash = cap_txt.rindex("/")
                free_txt = cap_txt[:slash].strip()
                cap_txt2 = cap_txt[slash + 1:].strip()
                free_val = int(re.sub(r"[^\d]", "", free_txt)) if free_txt else None
                cap_val = int(re.sub(r"[^\d]", "", cap_txt2)) if cap_txt2 else None
            except Exception:
                pass

        # waitlist
        waitlist = None
        wl_div = row.select_one('td.capacity div[title*="Anzahl Warteliste"]')
        if wl_div:
            span = wl_div.find("span")
            waitlist = (span.get_text(" ", strip=True) if span else wl_div.get_text(" ", strip=True)).strip()

        out.append({
            "pp": str(pp_id),
            "lv": str(lv_id),
            "title": title,
            "lecturers": _split_lecturers(prof),
            "semester": semester,
            "status": status,
            "capacity": cap_val,
            "free": free_val,
            "waitlist": waitlist,
        })

        if cap and len(out) >= cap:
            return True  # stop early
    return False

def _build_index(username: str, password: str) -> List[Dict[str, Any]]:
    """Full index build for a user account; returns flat list of all LVs."""
    start = _now()
    client = get_lpis_client(username, password)

    # Reach overview and submit once to show PP table
    try:
        client.ensure_overview()
    except Exception:
        pass

    selected = False
    try:
        client.browser.select_form("ea_stupl")
        selected = True
    except Exception:
        for frm in client.browser.forms():
            try:
                client.browser.form = frm
                _ = client.browser.form.find_control("ASPP")
                selected = True
                break
            except Exception:
                continue
    if not selected:
        raise HTTPException(status_code=502, detail="Could not reach study-plan form (ea_stupl / ASPP).")

    try:
        item = client.browser.form.find_control("ASPP").get(None, None, None, 0)
        item.selected = True
    except Exception:
        pass

    r = client.browser.submit()
    soup = BeautifulSoup(r.read(), "html.parser")

    # Iterate PP rows and gather LV rows; stop on time budget
    items: List[Dict[str, Any]] = []
    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    for planpunkt in rows:
        if (_now() - start) > REBUILD_TIME_BUDGET:
            break  # respect rebuild time budget

        a_tag = planpunkt.find("a")
        if not (a_tag and a_tag.get("id")):
            continue
        pp_id = a_tag["id"][1:]

        link_lv = planpunkt.select_one('a[href*="DLVO"]')
        if not link_lv:
            continue
        lv_url_rel = (link_lv.get("href", "") or "").strip()
        if not lv_url_rel:
            continue

        res2 = client.browser.open(client.URL_scraped + lv_url_rel)
        soup_lv = BeautifulSoup(res2.read(), "html.parser")
        # parse all rows for this PP (no query filter in the builder)
        _parse_lv_rows_fast(pp_id, soup_lv, tokens=[], cap=None, out=items)

    return items

def _ensure_index(username: str, password: str, force: bool = False):
    """Ensure we have a (fresh) index; trigger rebuild in background if needed."""
    with _CACHE_LOCK:
        entry = _CACHE.get(username)
        if not force and entry and _is_fresh(entry):
            return  # already fresh

        # If a rebuild is already running, don't start another
        if entry and entry.get("building"):
            return

        if not entry:
            entry = {"items": [], "updated": 0.0, "building": False,
                     "last_error": None, "build_started": None, "build_finished": None}
            _CACHE[username] = entry

        def _worker():
            entry["building"] = True
            entry["last_error"] = None
            entry["build_started"] = _now()
            entry["build_finished"] = None
            try:
                items = _build_index(username, password)
                entry["items"] = items
                entry["updated"] = _now()
            except Exception as e:
                entry["last_error"] = str(e)[:500]
            finally:
                entry["building"] = False
                entry["build_finished"] = _now()

        threading.Thread(target=_worker, daemon=True).start()

def _provisional_scan(username: str, password: str, q: str, limit: Optional[int], timeout_ms: int = 900) -> List[Dict[str, Any]]:
    """
    Quick, best-effort scan for first-time requests so we don't return empty while cache builds.
    Hard time-bounded; returns whatever it finds within timeout_ms.
    """
    start = _now()
    client = get_lpis_client(username, password)

    # reach overview
    try:
        client.ensure_overview()
    except Exception:
        pass

    # select form
    selected = False
    try:
        client.browser.select_form("ea_stupl")
        selected = True
    except Exception:
        for frm in client.browser.forms():
            try:
                client.browser.form = frm
                _ = client.browser.form.find_control("ASPP")
                selected = True
                break
            except Exception:
                continue
    if not selected:
        return []

    try:
        item = client.browser.form.find_control("ASPP").get(None, None, None, 0)
        item.selected = True
    except Exception:
        pass

    r = client.browser.submit()
    soup = BeautifulSoup(r.read(), "html.parser")

    tokens = [_norm(t) for t in (q or "").split() if t]
    cap = int(limit) if (isinstance(limit, int) and limit and limit > 0) else 10
    out: List[Dict[str, Any]] = []

    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    for planpunkt in rows:
        if (_now() - start) * 1000 > timeout_ms:
            break  # time budget exhausted

        a_tag = planpunkt.find("a")
        if not (a_tag and a_tag.get("id")):
            continue
        pp_id = a_tag["id"][1:]

        link_lv = planpunkt.select_one('a[href*="DLVO"]')
        if not link_lv:
            continue
        lv_url_rel = (link_lv.get("href", "") or "").strip()
        if not lv_url_rel:
            continue

        try:
            res2 = client.browser.open(client.URL_scraped + lv_url_rel)
            soup_lv = BeautifulSoup(res2.read(), "html.parser")
        except Exception:
            continue

        # parse minimal LV rows and apply filter
        if _parse_lv_rows_fast(pp_id, soup_lv, tokens, cap, out):
            break

    return out

# ---------------- Endpoints ----------------

@app.get("/")
def root():
    return {"greeting": "Hello, World!", "message": "Welcome to FastAPI!"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/courses/search")
def courses_search(p: SearchIn):
    """
    Ultra-fast on warm cache. If cache is cold or building, return a provisional
    result gathered within ~900 ms so the UI never shows an empty list.
    Query semantics:
      - AND over tokens; matches title + lecturers + LV id.
      - limit=0 or None => no cap.
    """
    try:
        # Kick off (or continue) background build, but don't block.
        _ensure_index(p.username, p.password, force=False)

        # Snapshot current cache
        with _CACHE_LOCK:
            entry = _CACHE.get(p.username) or {"items": [], "updated": 0.0, "building": False,
                                               "last_error": None, "build_started": None, "build_finished": None}
            items_snapshot = list(entry.get("items", []))
            updated = entry.get("updated", 0.0)
            building = bool(entry.get("building"))
            last_error = entry.get("last_error")

        # Filter snapshot
        tokens = [_norm(t) for t in (p.q or "").split() if t]
        cap = int(p.limit) if (isinstance(p.limit, int) and p.limit and p.limit > 0) else None

        out: List[Dict[str, Any]] = []
        for it in items_snapshot:
            if _matches(tokens, it.get("title") or "", " ".join(it.get("lecturers") or []), it.get("lv") or ""):
                out.append(it)
                if cap and len(out) >= cap:
                    break

        # If snapshot is empty (or not fresh) and we have a query, do a quick provisional scan
        provisional_used = False
        if (not out) and tokens:
            try:
                # Hard cap ~900ms; tune to 700–1200ms
                prov = _provisional_scan(p.username, p.password, p.q, p.limit, timeout_ms=900)
                if prov:
                    out = prov[: (cap or len(prov))]
                    provisional_used = True
            except Exception:
                pass

        return {
            "ok": True,
            "items": out,
            "meta": {
                "cached": bool(items_snapshot),
                "updated_at_unix": updated,
                "building": building,
                "fresh": (_now() - updated) < INDEX_TTL_SECONDS if updated else False,
                "provisional": provisional_used,
                "last_error": last_error,
            }
        }

    except HTTPException as he:
        return JSONResponse(status_code=he.status_code, content={"ok": False, "error": he.detail})
    except Exception as e:
        tb = traceback.format_exc(limit=5)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "trace": tb[:4000]})

@app.post("/courses/reindex")
def courses_reindex(p: ReindexIn):
    """
    Force a background reindex for this account. Returns immediately.
    Frontend can call this right after storing credentials, and a cron can call it every N minutes.
    """
    try:
        _ensure_index(p.username, p.password, force=True)
        return {"ok": True, "queued": True}
    except HTTPException as he:
        return JSONResponse(status_code=he.status_code, content={"ok": False, "error": he.detail})
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.get("/courses/index_status")
def index_status(username: str):
    with _CACHE_LOCK:
        entry = _CACHE.get(username)
        if not entry:
            return {"ok": True, "exists": False}
        return {
            "ok": True,
            "exists": True,
            "building": entry.get("building"),
            "updated_at_unix": entry.get("updated"),
            "items_cached": len(entry.get("items") or []),
            "fresh": _is_fresh(entry),
            "last_error": entry.get("last_error"),
            "build_started": entry.get("build_started"),
            "build_finished": entry.get("build_finished"),
        }

@app.post("/debug/structure")
def debug_structure(p: SearchIn):
    """
    TEMP endpoint to introspect what the scraper sees after login.
    Do not expose publicly in production.
    """
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")

    try:
        client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
        data = client.infos() if hasattr(client, "infos") else client.getResults()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"LPIS navigation failed: {e}")

    # Normalize potential structures
    normalized = data
    if hasattr(client, "getResults") and "data" not in normalized:
        try:
            normalized = client.getResults()
        except Exception:
            pass

    dat = (normalized or {}).get("data") or normalized or {}
    pp = dat.get("pp") or {}
    total_lvs = sum(len((v or {}).get("lvs") or {}) for v in pp.values())

    return {
        "ok": True,
        "studies_count": dat.get("studies_count"),
        "pp_count": len(pp),
        "lv_total": total_lvs,
        "sample_pp_ids": list(pp.keys())[:5],
    }

@app.post("/debug/forms")
def debug_forms(p: SearchIn):
    """
    Lists form names and their controls from the current mechanize context after ensure_overview().
    Helpful when LPIS instances differ in form naming.
    """
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")

    client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
    # open post-login base and try to reach overview
    try:
        client.ensure_overview()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ensure_overview missing or failed: {e}")

    forms_info = []
    for frm in client.browser.forms():
        try:
            controls = []
            for c in frm.controls:
                try:
                    controls.append(getattr(c, "name", None))
                except Exception:
                    controls.append(None)
            forms_info.append({"name": frm.name, "controls": controls})
        except Exception:
            forms_info.append({"name": None, "controls": []})
    return {"ok": True, "forms": forms_info}
