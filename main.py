# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import unicodedata, traceback, re, time, threading
from bs4 import BeautifulSoup  # already in your env

app = FastAPI()

# ---------------- Config ----------------
INDEX_TTL_SECONDS = 600  # 10 minutes cache TTL; tune as needed
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
    # optional: restrict to given PP ids to speed up a partial refresh
    pp_ids: Optional[List[str]] = None

# --------------- Cache ------------------
# CACHE: { username: { "items": List[dict], "updated": float_ts, "building": bool } }
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()

def _now() -> float:
    return time.time()

def _is_fresh(entry: Dict[str, Any]) -> bool:
    return entry and ( _now() - entry.get("updated", 0) ) < INDEX_TTL_SECONDS

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
def _parse_lv_table(pp_id: str, soup_lv: BeautifulSoup) -> List[Dict[str, Any]]:
    rows_out: List[Dict[str, Any]] = []
    lv_table = soup_lv.find("table", {"class": "b3k-data"})
    lv_body = lv_table.find("tbody") if lv_table else None
    if not lv_body:
        return rows_out

    for row in lv_body.find_all("tr"):
        ver_id_link = row.select_one(".ver_id a")
        if not ver_id_link:
            continue
        lv_id = (ver_id_link.get_text(" ", strip=True) or "").strip()
        if not lv_id:
            continue

        sem_span = row.select_one(".ver_id span")
        semester = (sem_span.get_text(" ", strip=True) if sem_span else None)

        prof_div = row.select_one(".ver_title div")
        prof = (prof_div.get_text(" ", strip=True) if prof_div else "").strip()

        name_td = row.find("td", {"class": "ver_title"})
        title = ""
        if name_td:
            title_el = name_td.select_one("a, strong, span")
            title = (title_el.get_text(" ", strip=True) if title_el else name_td.get_text(" ", strip=True)).strip()
            if prof:
                title = re.sub(re.escape(prof) + r"\s*$", "", title).strip(" -·•\u00A0")
            title = re.sub(r"\s+", " ", title).strip()

        status_div = row.select_one("td.box div")
        status = (status_div.get_text(" ", strip=True) if status_div else None)

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

        waitlist = None
        wl_div = row.select_one('td.capacity div[title*="Anzahl Warteliste"]')
        if wl_div:
            span = wl_div.find("span")
            waitlist = (span.get_text(" ", strip=True) if span else wl_div.get_text(" ", strip=True)).strip()

        rows_out.append({
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
    return rows_out

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
        items.extend(_parse_lv_table(pp_id, soup_lv))

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
            entry = {"items": [], "updated": 0.0, "building": False}
            _CACHE[username] = entry

        def _worker():
            try:
                entry["building"] = True
                items = _build_index(username, password)
                entry["items"] = items
                entry["updated"] = _now()
            except Exception:
                # keep old snapshot on failure
                pass
            finally:
                entry["building"] = False

        threading.Thread(target=_worker, daemon=True).start()

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
    Ultra-fast: returns from cache (ms). If cache is stale/missing, triggers a background rebuild.
    Query semantics:
      - AND over tokens; matches title + lecturers + LV id.
      - limit=0 or None => no cap.
    """
    try:
        # Kick off a rebuild if needed, but do NOT block the response.
        _ensure_index(p.username, p.password, force=False)

        # Read current snapshot
        with _CACHE_LOCK:
            entry = _CACHE.get(p.username) or {"items": [], "updated": 0.0, "building": False}
            items_snapshot = list(entry.get("items", []))
            updated = entry.get("updated", 0.0)
            building = bool(entry.get("building"))

        tokens = [_norm(t) for t in (p.q or "").split() if t]
        cap = int(p.limit) if (isinstance(p.limit, int) and p.limit and p.limit > 0) else None

        out: List[Dict[str, Any]] = []
        for it in items_snapshot:
            if _matches(tokens, it.get("title") or "", " ".join(it.get("lecturers") or []), it.get("lv") or ""):
                out.append(it)
                if cap and len(out) >= cap:
                    break

        return {
            "ok": True,
            "items": out,
            "meta": {
                "cached": True,
                "updated_at_unix": updated,
                "building": building,
                "fresh": ( _now() - updated ) < INDEX_TTL_SECONDS
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

@app.post("/debug/structure")
def debug_structure(p: SearchIn):
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")

    try:
        client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
        data = client.infos() if hasattr(client, "infos") else client.getResults()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"LPIS navigation failed: {e}")

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
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")

    client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
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
