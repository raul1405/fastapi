# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import unicodedata
import traceback
import re

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
    Normalize LPIS structure to a flat course list used by the UI.
    - Matches on course title, lecturer(s), and LV id.
    - Multi-word query uses AND semantics.
    - limit=0 or None means 'no cap'.
    """
    items: List[Dict[str, Any]] = []

    # result can be either:
    #  - {"data": {"pp": {...}}, "status": {...}}  (from getResults())
    #  - {"pp": {...}, ...}                        (direct infos() return)
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


# ---------- Endpoints ----------
@app.post("/courses/search")
def courses_search(p: SearchIn):
    """
    Logs in to LPIS, scrapes the study plan, filters LVs by 'q', returns a flat list.
    - Matches title + lecturer + lv id (accent-insensitive).
    - Multi-word queries are ANDed.
    - limit=0 (or null) means 'no cap'.
    """
    try:
        client = get_lpis_client(p.username, p.password)  # may raise HTTPException(500/502)

        # first try infos(), else getResults()
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
