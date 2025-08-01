# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup
import unicodedata
import traceback
import threading
import socket
import contextlib
import re
import time

# ---------- Global network defaults ----------
# Hard default so requests can't hang forever (used outside provisional path)
socket.setdefaulttimeout(8)

app = FastAPI()

# ---------- Config ----------
INDEX_TTL_SECONDS = 600            # 10 minutes cache TTL
REBUILD_TIME_BUDGET = 25           # seconds budget for full index build
PROVISIONAL_TIMEOUT_MS = 4500      # ~4.5s overall budget for provisional scan
PROVISIONAL_NET_TIMEOUT = 3.0      # per-request timeout for LV page fetches inside provisional

# ---------- Models ----------
class SearchIn(BaseModel):
    username: str
    password: str
    q: str = ""
    limit: Optional[int] = 20

class ReindexIn(BaseModel):
    username: str
    password: str
    pp_ids: Optional[List[str]] = None  # reserved

class EnrollIn(BaseModel):
    username: str
    password: str
    pp: str
    lv: str
    group_id: Optional[str] = None
    auto_waitlist: bool = True

# ---------- In-memory cache ----------
# _CACHE: {
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

# ---------- Text utils ----------
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
    return all(t in hay_n for t in tokens)  # strict AND

# ---------- Timeout helper for provisional path ----------
@contextlib.contextmanager
def _temp_socket_timeout(sec: float):
    """Temporarily change the global socket default timeout."""
    old = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(sec)
        yield
    finally:
        socket.setdefaulttimeout(old)

# ---------- LPIS client loader ----------
def get_lpis_client(user: str, pw: str):
    try:
        from lpislib import WuLpisApi  # vendored client
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")
    try:
        return WuLpisApi(user, pw, args=None, sessiondir=None)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

# ---------- Low-level LV table parse (fast) ----------
def _parse_lv_rows_fast(pp_id: str, soup_lv: BeautifulSoup, tokens: List[str],
                        cap: Optional[int], out: List[Dict[str, Any]]) -> bool:
    """
    Parse a PP's LV table quickly and append only matching rows to 'out'.
    Robust title extraction for LPIS variants. Returns True if cap reached.
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

        # -------- robust title extraction --------
        name_td = row.find("td", {"class": "ver_title"})
        title = ""
        fallback_full = ""
        if name_td:
            fallback_full = (name_td.get_text(" ", strip=True) or "").strip()

            # prefer bare text nodes outside children
            bare_texts = [t.strip() for t in name_td.find_all(string=True, recursive=False) if (t or "").strip()]
            if bare_texts:
                title = bare_texts[-1]

            if not title:
                el = name_td.select_one("a, strong, span")
                if el:
                    title = (el.get_text(" ", strip=True) or "").strip()

            if title and prof and _norm(title) == _norm(prof):
                title = ""

            if (not title) and fallback_full:
                if prof and fallback_full.lower().endswith(prof.lower()):
                    title = re.sub(re.escape(prof) + r"\s*$", "", fallback_full).strip(" -·•\u00A0")
                else:
                    title = fallback_full

            title = re.sub(r"\s+", " ", title or "").strip()
        # ----------------------------------------

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

        # ─── original waitlist string ────────────────────────────────
        waitlist = None
        wl_div = row.select_one('td.capacity div[title*="Anzahl Warteliste"]')
        if wl_div:
            span = wl_div.find("span")
            waitlist = (span.get_text(" ", strip=True) if span else wl_div.get_text(" ", strip=True)).strip()

        # ---- matching ----
        if tokens:
            hay = " ".join(filter(None, [title, prof, lv_id, fallback_full]))
            hay_n = _norm(hay)
            if not all(t in hay_n for t in tokens):
                continue
            waitlist = (span.get_text(" ", strip=True)
                        if span else wl_div.get_text(" ", strip=True)).strip()

        # ─── parse waitlist_count as integer ──────────────────────────
        try:
            waitlist_count = int(''.join(filter(str.isdigit, waitlist))) if waitlist else 0
        except Exception:
            waitlist_count = 0

        # ─── parse enroll_open_at timestamp from "ab DD.MM.YYYY HH:MM" ─
        enroll_open_at = None
        action_td = row.select_one("td.action")
        if action_td:
            txt = action_td.get_text(" ", strip=True)
            m = re.search(r"ab\s*(\d{2}\.\d{2}\.\d{4}\s*\d{2}:\d{2})", txt)
            if m:
                # convert "18.08.2025 15:00" → "2025-08-18T15:00:00Z"
                try:
                    day, month, year_time = m.group(1).split('.', 2)
                    year, timepart = year_time.split(' ',1)
                    dt_iso = f"{year}-{month}-{day}T{timepart}:00Z"
                    enroll_open_at = dt_iso
                except:
                    enroll_open_at = m.group(1)  # fallback raw string

        # ─── append item with new fields ──────────────────────────────
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
            "waitlist": waitlist,             # original label
            "waitlist_count": waitlist_count, # integer count
            "enroll_open_at": enroll_open_at, # ISO timestamp or raw
        })


        if cap and len(out) >= cap:
            return True
    return False

# ---------- Full index build (background) ----------
def _build_index(username: str, password: str) -> List[Dict[str, Any]]:
    """Full index build for a user account; returns flat list of all LVs."""
    start = _now()
    client = get_lpis_client(username, password)

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

    items: List[Dict[str, Any]] = []
    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    for planpunkt in rows:
        if (_now() - start) > REBUILD_TIME_BUDGET:
            break

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
        except Exception:
            continue
        soup_lv = BeautifulSoup(res2.read(), "html.parser")
        _parse_lv_rows_fast(pp_id, soup_lv, tokens=[], cap=None, out=items)

    return items

def _ensure_index(username: str, password: str, force: bool = False):
    """Ensure we have a (fresh) index; trigger rebuild in background if needed."""
    with _CACHE_LOCK:
        entry = _CACHE.get(username)
        if not force and entry and _is_fresh(entry):
            return

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

# ---------- Provisional scan (staged fast path) ----------
def _provisional_scan(username: str, password: str, q: str, limit: Optional[int],
                      timeout_ms: int = PROVISIONAL_TIMEOUT_MS) -> List[Dict[str, Any]]:
    """
    Staged fast path:
      - Login & first submit with slightly higher timeout (~5s) to survive TLS/redirects
      - LV page fetches with short timeout (~3.0s) to keep total within a few seconds
    """
    LOGIN_TIMEOUT = 5.0
    PAGE_TIMEOUT  = PROVISIONAL_NET_TIMEOUT  # 3.0
    start = _now()
    out: List[Dict[str, Any]] = []

    # 1) Login + reach PP table
    with _temp_socket_timeout(LOGIN_TIMEOUT):
        client = get_lpis_client(username, password)
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
            return []

        try:
            item = client.browser.form.find_control("ASPP").get(None, None, None, 0)
            item.selected = True
        except Exception:
            pass

        try:
            r = client.browser.submit()
        except Exception:
            return []
        soup = BeautifulSoup(r.read(), "html.parser")

    # 2) Choose candidate PPs
    tokens = [_norm(t) for t in (q or "").split() if t]
    cap = int(limit) if (isinstance(limit, int) and limit and limit > 0) else 10

    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    candidates = []  # (priority, pp_id, lv_url_rel)
    for planpunkt in rows:
        if (_now() - start) * 1000 > timeout_ms:
            break

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

        span1 = planpunkt.select_one("td:nth-of-type(1) span:nth-of-type(1)")
        span2 = planpunkt.select_one("td:nth-of-type(1) span:nth-of-type(2)")
        pp_type = (span1.get_text(" ", strip=True) if span1 else "").strip()
        pp_name = (span2.get_text(" ", strip=True) if span2 else "").strip()
        if not pp_name:
            second_td = planpunkt.select_one("td:nth-of-type(2)")
            pp_name = (second_td.get_text(" ", strip=True) if second_td else "").strip()

        hay = _norm(" ".join([pp_type, pp_name]))
        priority = 0 if (tokens and any(t in hay for t in tokens)) else 1

        candidates.append((priority, pp_id, lv_url_rel))

    candidates.sort(key=lambda x: x[0])
    candidates = candidates[:6] if tokens else candidates[:3]

    # 3) Open LV pages quickly and parse
    with _temp_socket_timeout(PAGE_TIMEOUT):
        for _prio, pp_id, lv_url_rel in candidates:
            if (_now() - start) * 1000 > timeout_ms:
                break
            try:
                try:
                    res2 = client.browser.open(client.URL_scraped + lv_url_rel, timeout=PAGE_TIMEOUT)
                except TypeError:
                    res2 = client.browser.open(client.URL_scraped + lv_url_rel)
                soup_lv = BeautifulSoup(res2.read(), "html.parser")
            except Exception:
                continue

            if _parse_lv_rows_fast(pp_id, soup_lv, tokens, cap, out):
                break

        # relaxed OR pass (tiny) if still empty and time left
        if not out and tokens and ((_now() - start) * 1000) <= timeout_ms:
            for _prio, pp_id, lv_url_rel in candidates[:2]:
                if (_now() - start) * 1000 > timeout_ms:
                    break
                try:
                    try:
                        res2 = client.browser.open(client.URL_scraped + lv_url_rel, timeout=PAGE_TIMEOUT)
                    except TypeError:
                        res2 = client.browser.open(client.URL_scraped + lv_url_rel)
                    soup_lv = BeautifulSoup(res2.read(), "html.parser")
                except Exception:
                    continue

                tmp: List[Dict[str, Any]] = []
                _parse_lv_rows_fast(pp_id, soup_lv, tokens=[], cap=None, out=tmp)

                for it in tmp:
                    hay = _norm(" ".join(filter(None, [
                        it.get("title") or "",
                        " ".join(it.get("lecturers") or []),
                        str(it.get("lv") or "")
                    ])))
                    if any(t in hay for t in tokens):
                        out.append(it)
                        if cap and len(out) >= cap:
                            break

    return out

# ---------- Enrollment helpers ----------
def _reach_pp_lv_page(client, pp_id: str) -> BeautifulSoup:
    """
    After login/ensure_overview(): show plan table and open DLVO page for given PP.
    """
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
        raise HTTPException(status_code=502, detail="Could not reach study-plan form (ea_stupl/ASPP).")

    try:
        item = client.browser.form.find_control("ASPP").get(None, None, None, 0)
        item.selected = True
    except Exception:
        pass

    r = client.browser.submit()
    soup = BeautifulSoup(r.read(), "html.parser")

    # find DLVO link for PP
    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    dlvo_href = None
    for planpunkt in rows:
        a_tag = planpunkt.find("a")
        if not (a_tag and a_tag.get("id")):
            continue
        cur_pp = a_tag["id"][1:]  # 'S12345' -> '12345'
        if cur_pp != str(pp_id):
            continue
        link_lv = planpunkt.select_one('a[href*="DLVO"]')
        if link_lv:
            dlvo_href = (link_lv.get("href") or "").strip()
            break

    # fallback via infos()
    if not dlvo_href:
        try:
            data = client.infos()
            pp_map = (data or {}).get("pp") or {}
            entry = pp_map.get(str(pp_id)) or pp_map.get(pp_id)
            if entry and entry.get("lv_url"):
                dlvo_href = entry["lv_url"]
        except Exception:
            pass

    if not dlvo_href:
        raise HTTPException(status_code=404, detail=f"PP {pp_id} not found or no LV link.")

    res2 = client.browser.open(client.URL_scraped + dlvo_href)
    return BeautifulSoup(res2.read(), "html.parser")

def _submit_enroll_on_lv_page(client, soup_lv: BeautifulSoup, lv_id: str,
                              group_id: Optional[str], auto_waitlist: bool) -> Dict[str, Any]:
    """
    Find LV row (lv_id), pick its form and submit.
    Handles pre-closed status and 2-step confirm pages.
    """
    lv_table = soup_lv.find("table", {"class": "b3k-data"})
    lv_body = lv_table.find("tbody") if lv_table else None
    if not lv_body:
        raise HTTPException(status_code=502, detail="LV table not present on DLVO page.")

    target_form_name = None
    pre_status_txt = ""
    for row in lv_body.find_all("tr"):
        ver_id_link = row.select_one(".ver_id a")
        if not ver_id_link:
            continue
        cur_lv = (ver_id_link.get_text(" ", strip=True) or "").strip()
        if cur_lv != str(lv_id):
            continue

        # pre status (e.g. "Anmeldung nicht möglich")
        status_div = row.select_one("td.box div")
        pre_status_txt = (status_div.get_text(" ", strip=True) if status_div else "").strip().lower()

        form = row.select_one("td.action form")
        if form and form.get("name"):
            target_form_name = form["name"]
        break

    if not target_form_name:
        raise HTTPException(status_code=404, detail=f"LV {lv_id} not found or no enroll form present.")

    # short-circuit: closed
    if any(k in pre_status_txt for k in ["nicht möglich", "gesperrt", "geschlossen"]):
        return {"result": "closed", "message": "Anmeldung derzeit nicht möglich (laut LV-Status)."}

    # pick the matching form
    try:
        client.browser.select_form(target_form_name)
    except Exception:
        matched = False
        for frm in client.browser.forms():
            if getattr(frm, "name", None) == target_form_name:
                client.browser.form = frm
                matched = True
                break
        if not matched:
            raise HTTPException(status_code=502, detail="Enroll form not selectable in mechanize context.")

    # optional group
    if group_id:
        for possible in ["GRUPPE", "group", "GROUP", "grp", "gruppe", "GRP_ID", "GRUPPE_ID"]:
            try:
                ctrl = client.browser.form.find_control(possible)
                try:
                    ctrl.value = [group_id]
                except Exception:
                    try:
                        ctrl.value = group_id
                    except Exception:
                        pass
            except Exception:
                continue

    # optional waitlist selection
    if auto_waitlist:
        for ctrl in getattr(client.browser.form, "controls", []):
            try:
                if hasattr(ctrl, "items") and ctrl.items:
                    for item in ctrl.items:
                        try:
                            labels = []
                            try:
                                labels = [l.text.lower() for l in item.get_labels()]
                            except Exception:
                                pass
                            if any("wart" in (lbl or "") for lbl in labels):
                                item.selected = True
                        except Exception:
                            continue
                val = getattr(ctrl, "value", None)
                if isinstance(val, str) and "wart" in val.lower():
                    ctrl.value = val
            except Exception:
                continue

    # first submit
    r3 = client.browser.submit()
    soup3 = BeautifulSoup(r3.read(), "html.parser")
    page_text = soup3.get_text(" ", strip=True).lower()

    # detect confirm step
    confirm_needed = any(k in page_text for k in ["bestätigen", "bestaetigen", "überprüfen", "ueberpruefen"]) and not any(
        k in page_text for k in ["erfolgreich", "warteliste", "bereits angemeldet"]
    )

    if confirm_needed:
        picked = False
        try:
            for frm in client.browser.forms():
                nm = (getattr(frm, "name", "") or "").lower()
                if any(x in nm for x in ["bestaet", "bestät", "confirm"]):
                    client.browser.form = frm
                    picked = True
                    break
        except Exception:
            pass

        if not picked:
            try:
                client.browser.form = next(iter(client.browser.forms()))
                picked = True
            except Exception:
                picked = False

        if picked:
            try:
                r4 = client.browser.submit()
                soup4 = BeautifulSoup(r4.read(), "html.parser")
                page_text = soup4.get_text(" ", strip=True).lower()
            except Exception:
                pass

    if any(k in page_text for k in ["erfolgreich angemeldet", "anmeldung erfolgreich", "erfolgreich durchgef"]):
        return {"result": "success", "message": "Anmeldung erfolgreich."}
    if "warteliste" in page_text or "auf die warteliste" in page_text:
        return {"result": "waitlist", "message": "Auf Warteliste eingetragen."}
    if any(k in page_text for k in ["bereits angemeldet", "schon angemeldet"]):
        return {"result": "already", "message": "Bereits angemeldet."}
    if any(k in page_text for k in ["nicht möglich", "gesperrt", "geschlossen"]):
        return {"result": "closed", "message": "Anmeldung derzeit nicht möglich."}

    # unknown
    forms = []
    try:
        forms = [getattr(f, "name", None) for f in client.browser.forms()]
    except Exception:
        pass
    snippet = page_text[:600]
    return {
        "result": "unknown",
        "message": "Status unklar – bitte im LPIS prüfen.",
        "debug": {"snippet": snippet, "forms": forms}
    }

# ---------- Endpoints ----------
@app.get("/")
def root():
    return {"greeting": "Hello, World!", "message": "Welcome to FastAPI!"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/courses/reindex_sync")
def courses_reindex_sync(p: ReindexIn):
    if not p.username or not p.password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    try:
        print(f"[reindex_sync] Starting full build for {p.username}")
        start = _now()
        items = _build_index(p.username, p.password)

        with _CACHE_LOCK:
            _CACHE[p.username] = {
                "items": items,
                "updated": _now(),
                "building": False,
                "last_error": None,
                "build_started": start,
                "build_finished": _now(),
            }

        elapsed_ms = int((_now() - start) * 1000)
        print(f"[reindex_sync] Built {len(items)} items in {elapsed_ms} ms for {p.username}")

        return {"ok": True, "seeded": len(items), "ms": elapsed_ms}

    except Exception as e:
        tb = traceback.format_exc(limit=5)
        print(f"[reindex_sync] Error: {e}\n{tb}")
        return JSONResponse(status_code=502, content={"ok": False, "error": str(e)})

# --- SEARCH (cache + provisional + relaxed fallback) ---
@app.post("/courses/search")
def courses_search(p: SearchIn):
    """
    Ultra-fast on warm cache. If cache is cold or strict filter yields nothing:
    - run provisional scan (staged timeouts),
    - then try a relaxed OR-match on cache,
    - finally a broad provisional scan and OR-filter (fallback).
    """
    try:
        _ensure_index(p.username, p.password, force=False)

        with _CACHE_LOCK:
            entry = _CACHE.get(p.username) or {
                "items": [], "updated": 0.0, "building": False,
                "last_error": None, "build_started": None, "build_finished": None
            }
            items_snapshot = list(entry.get("items", []))
            updated = entry.get("updated", 0.0)
            building = bool(entry.get("building"))
            last_error = entry.get("last_error")

        tokens = [_norm(t) for t in (p.q or "").split() if t]
        cap = int(p.limit) if (isinstance(p.limit, int) and p.limit and p.limit > 0) else None

        # Pass 1: strict AND-match on cache
        # Pass 1: substring search (diacritic‑folding + case‑insensitive)
        import unicodedata

        def norm_sub(s: str) -> str:
            return (
                unicodedata
                .normalize("NFD", s)
                .encode("ascii", "ignore")
                .decode("ascii")
                .lower()
            )

        qnorm = norm_sub(p.q or "")
        out: List[Dict[str, Any]] = []
        if qnorm:
            for it in items_snapshot:
                title_norm = norm_sub(it.get("title", ""))
                code_norm  = (it.get("pp", "") + it.get("lv", "")).lower()
                if qnorm in title_norm or qnorm in code_norm:
                    out.append(it)
                    if cap and len(out) >= cap:
                        break
        else:
            # empty query → return everything
            out = items_snapshot

        # Provisional scan if nothing found and user provided tokens
        provisional_used = False
        prov_error = None
        if (not out) and tokens:
            provisional_used = True
            try:
                prov = _provisional_scan(p.username, p.password, p.q, p.limit, timeout_ms=PROVISIONAL_TIMEOUT_MS)
                if prov:
                    out = prov[: (cap or len(prov))]
            except Exception as e:
                prov_error = str(e)

        # Pass 2: relaxed OR-match on cache if still empty
        if (not out) and tokens and items_snapshot:
            relaxed: List[Dict[str, Any]] = []
            for it in items_snapshot:
                hay = " ".join(filter(None, [
                    it.get("title") or "",
                    " ".join(it.get("lecturers") or []),
                    str(it.get("lv") or "")
                ]))
                hay_n = _norm(hay)
                if any(t in hay_n for t in tokens):   # OR instead of AND
                    relaxed.append(it)
                    if cap and len(relaxed) >= cap:
                        break
            if relaxed:
                out = relaxed

        # Pass 3: broad provisional scan then OR-filter (final fallback while cache builds)
        if (not out) and tokens:
            try:
                broad = _provisional_scan(p.username, p.password, "", max(p.limit or 20, 50),
                                          timeout_ms=PROVISIONAL_TIMEOUT_MS)
                if broad:
                    filtered = []
                    for it in broad:
                        hay = " ".join(filter(None, [
                            it.get("title") or "",
                            " ".join(it.get("lecturers") or []),
                            str(it.get("lv") or "")
                        ]))
                        hay_n = _norm(hay)
                        if any(t in hay_n for t in tokens):  # OR-match
                            filtered.append(it)
                            if cap and len(filtered) >= cap:
                                break
                    if filtered:
                        out = filtered
            except Exception as e:
                prov_error = prov_error or str(e)

        return {
            "ok": True,
            "items": out,
            "meta": {
                "cached": bool(items_snapshot),
                "updated_at_unix": updated,
                "building": building,
                "fresh": (_now() - updated) < INDEX_TTL_SECONDS if updated else False,
                "provisional": True if ((not items_snapshot) or provisional_used) else False,
                "last_error": prov_error or last_error,
            }
        }

    except HTTPException as he:
        return JSONResponse(status_code=he.status_code, content={"ok": False, "error": he.detail})
    except Exception as e:
        tb = traceback.format_exc(limit=5)
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e), "trace": tb[:4000]})

@app.post("/courses/reindex_sync")
def reindex_sync(p: ReindexIn):
    if not p.username or not p.password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    try:
        # Debug log before building
        print(f"[reindex_sync] Starting full build for {p.username}")
        items = _build_index(p.username, p.password)
        # Atomically replace cache
        with _CACHE_LOCK:
            _CACHE[p.username] = {
                "items": items,
                "updated": _now(),
                "building": False,
                "last_error": None,
                "build_started": None,
                "build_finished": _now(),
            }
        # Debug log after building
        print(f"[reindex_sync] Built {len(items)} items for {p.username}")
        return {"ok": True, "seeded": len(items)}
    except Exception as e:
        tb = traceback.format_exc(limit=5)
        print(f"[reindex_sync] ERROR: {e}\n{tb}")
        raise HTTPException(status_code=502, detail=str(e))


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

# ---------- DEBUG ----------
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

@app.post("/debug/provisional")
def debug_provisional(p: SearchIn):
    """Show which PP LV pages were opened during a provisional-like scan."""
    LOGIN_TIMEOUT = 5.0
    PAGE_TIMEOUT  = PROVISIONAL_NET_TIMEOUT
    start = _now()
    tried: List[Dict[str, Any]] = []

    with _temp_socket_timeout(LOGIN_TIMEOUT):
        client = get_lpis_client(p.username, p.password)
        try:
            client.ensure_overview()
        except Exception:
            pass
        # reach form
        try:
            client.browser.select_form("ea_stupl")
        except Exception:
            for frm in client.browser.forms():
                try:
                    client.browser.form = frm
                    _ = client.browser.form.find_control("ASPP")
                    break
                except Exception:
                    continue
        try:
            item = client.browser.form.find_control("ASPP").get(None, None, None, 0)
            item.selected = True
        except Exception:
            pass
        r = client.browser.submit()
        soup = BeautifulSoup(r.read(), "html.parser")

    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    candidates = []
    for planpunkt in rows:
        a_tag = planpunkt.find("a")
        if not (a_tag and a_tag.get("id")):
            continue
        pp_id = a_tag["id"][1:]
        link_lv = planpunkt.select_one('a[href*="DLVO"]')
        if not link_lv:
            continue
        lv_url_rel = (link_lv.get("href", "") or "").strip()
        candidates.append((pp_id, lv_url_rel))
    candidates = candidates[:6]

    with _temp_socket_timeout(PAGE_TIMEOUT):
        for pp_id, lv_url_rel in candidates:
            rec = {"pp": pp_id, "href": lv_url_rel, "ok": False, "rows": 0, "error": None}
            try:
                try:
                    res2 = client.browser.open(client.URL_scraped + lv_url_rel, timeout=PAGE_TIMEOUT)
                except TypeError:
                    res2 = client.browser.open(client.URL_scraped + lv_url_rel)
                soup_lv = BeautifulSoup(res2.read(), "html.parser")
                lv_table = soup_lv.find("table", {"class": "b3k-data"})
                lv_body = lv_table.find("tbody") if lv_table else None
                rows_lv = lv_body.find_all("tr") if lv_body else []
                rec["rows"] = len(rows_lv)
                rec["ok"] = True
            except Exception as e:
                rec["error"] = str(e)[:180]
            tried.append(rec)

    return {"ok": True, "tried": tried, "took_ms": int((_now() - start) * 1000)}

# ---------- ENROLL ----------
@app.post("/enroll")
def enroll(p: EnrollIn):
    """
    Perform an enrollment for the given PP/LV.
    """
    if not p.username or not p.password or not p.pp or not p.lv:
        raise HTTPException(status_code=400, detail="Missing credentials or pp/lv.")

    try:
        client = get_lpis_client(p.username, p.password)
        try:
            client.ensure_overview()
        except Exception:
            pass

        soup_lv = _reach_pp_lv_page(client, str(p.pp))
        res = _submit_enroll_on_lv_page(client, soup_lv, str(p.lv), p.group_id, p.auto_waitlist)

        return {
            "ok": True,
            "result": res.get("result"),
            "message": res.get("message"),
            "payload": { "pp": str(p.pp), "lv": str(p.lv) }
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
