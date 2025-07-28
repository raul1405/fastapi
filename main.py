# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import unicodedata
import traceback
import re

from bs4 import BeautifulSoup  # already in your worker env

app = FastAPI()


# ---------- Health & Root ----------
@app.get("/")
def root():
    return {"greeting": "Hello, World!", "message": "Welcome to FastAPI!"}


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ---------- Models ----------
class SearchIn(BaseModel):
    username: str
    password: str
    q: str = ""
    limit: Optional[int] = 20


# ---------- Helpers ----------
def _norm(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _split_lecturers(prof: str) -> List[str]:
    """Split lecturer string into a clean list (handles · , ; / |)."""
    if not prof:
        return []
    parts = re.split(r"[·•|,;/]+", prof)
    return [p.strip() for p in parts if p and p.strip()]


def extract_items(result: Dict[str, Any], q: str, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    Normalize LPIS structure to a flat course list used by the UI (full scrape path).
    - Matches on course title, lecturer(s), and LV id.
    - Multi-word query uses AND semantics.
    - limit=0 or None means 'no cap'.
    """
    items: List[Dict[str, Any]] = []

    data = (result or {}).get("data") or result or {}
    pp_map = data.get("pp") or {}
    tokens = [_norm(t) for t in (q or "").split() if t]

    def matches(title: str, prof: str, lv_id: str) -> bool:
        if not tokens:
            return True
        hay = " ".join(filter(None, [title, prof, str(lv_id)]))
        hay_n = _norm(hay)
        return all(t in hay_n for t in tokens)

    cap = int(limit) if (isinstance(limit, int) and limit and limit > 0) else None

    for pp_id, pp_obj in pp_map.items():
        lvs = (pp_obj or {}).get("lvs") or {}
        for lv_id, lv in lvs.items():
            title = (lv or {}).get("name") or ""
            prof = (lv or {}).get("prof") or ""
            if not matches(title, prof, str(lv_id)):
                continue

            items.append({
                "pp": str(pp_id),
                "lv": str(lv_id),
                "title": title,
                "lecturers": _split_lecturers(prof),
                "semester": lv.get("semester"),
                "status": lv.get("status"),
                "capacity": lv.get("capacity"),
                "free": lv.get("free"),
                "waitlist": lv.get("waitlist"),
            })

            if cap and len(items) >= cap:
                return items
    return items


def get_lpis_client(user: str, pw: str):
    """
    Lazy import WuLpisApi and construct a client.
    Returns HTTPException with proper codes on failure so callers can propagate.
    """
    try:
        from lpislib import WuLpisApi  # vendored package
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")
    try:
        return WuLpisApi(user, pw, args=None, sessiondir=None)
    except Exception as e:
        # Login/redirect/parse errors → treat as upstream failure
        raise HTTPException(status_code=502, detail=str(e))


# ---------- FAST PATH ----------
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


def fast_scan(client, query: str, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    FAST path: navigate once to the overview and then open PP LV pages
    one-by-one, collecting matches and STOPPING when 'limit' is reached.
    Avoids building the full structure and avoids touching irrelevant PP pages.
    """
    tokens = [_norm(t) for t in (query or "").split() if t]
    cap = int(limit) if (isinstance(limit, int) and limit and limit > 0) else None
    out: List[Dict[str, Any]] = []

    # 1) Ensure we're at the study-plan page and submit the form once
    try:
        client.ensure_overview()
    except Exception as e:
        # If ensure_overview not available for some reason, fallback to full path
        raise

    # Select study-plan form or any form with ASPP, then submit to get PP table
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

    # 2) Iterate PP rows; for each with lv_url, open LV list and filter quickly.
    table = soup.find("table", {"class": "b3k-data"})
    tbody = table.find("tbody") if table else None
    rows = tbody.find_all("tr") if tbody else []

    for planpunkt in rows:
        a_tag = planpunkt.find("a")
        if not (a_tag and a_tag.get("id")):
            continue
        pp_id = a_tag["id"][1:]  # drop 'S'

        link_lv = planpunkt.select_one('a[href*="DLVO"]')
        if not link_lv:
            continue
        lv_url_rel = link_lv.get("href", "").strip()
        if not lv_url_rel:
            continue

        # Open LV page for this PP and parse only matching rows
        res2 = client.browser.open(client.URL_scraped + lv_url_rel)
        soup_lv = BeautifulSoup(res2.read(), "html.parser")

        if _parse_lv_rows_fast(pp_id, soup_lv, tokens, cap, out):
            break  # cap reached — stop scanning more PP pages

    return out


# ---------- Endpoints ----------
@app.post("/courses/search")
def courses_search(p: SearchIn):
    """
    Logs in to LPIS, then:
      - FAST PATH: if q is non-empty and limit > 0, scan PP pages progressively and stop early.
      - FULL PATH: otherwise, build full structure and filter.
    """
    try:
        client = get_lpis_client(p.username, p.password)  # may raise HTTPException(500/502)

        use_fast = bool((p.q or "").strip()) and isinstance(p.limit, int) and (p.limit or 0) > 0
        if use_fast:
            try:
                items = fast_scan(client, p.q, p.limit)
                return {"ok": True, "items": items}
            except Exception:
                # If anything odd happens in fast mode, fall back to reliable full mode
                pass

        # --- FULL PATH fallback ---
        if hasattr(client, "infos"):
            res = client.infos()
        elif hasattr(client, "getResults"):
            res = client.getResults()
        else:
            raise HTTPException(status_code=500, detail="LPIS client missing both infos() and getResults()")

        items = extract_items(res, p.q, p.limit)
        return {"ok": True, "items": items}

    except HTTPException as he:
        return JSONResponse(status_code=he.status_code, content={"ok": False, "error": he.detail})
    except Exception as e:
        tb = traceback.format_exc(limit=5)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e), "trace": tb[:4000]},
        )


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
