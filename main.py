# main.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import traceback

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
def extract_items(result: Dict[str, Any], q: str, limit: Optional[int]) -> List[Dict[str, Any]]:
    """
    Normalize LPIS structure to a flat course list used by the UI.
    """
    items: List[Dict[str, Any]] = []

    # result can be either:
    #  - {"data": {"pp": {...}}, "status": {...}}  (from getResults())
    #  - {"pp": {...}, ...}                        (direct infos() return)
    data = (result or {}).get("data") or result or {}
    pp_map = data.get("pp") or {}

    q_lower = (q or "").strip().lower()

    for pp_id, pp_obj in pp_map.items():
        lvs = (pp_obj or {}).get("lvs") or {}
        for lv_id, lv in lvs.items():
            title = (lv or {}).get("name") or ""
            prof = (lv or {}).get("prof") or ""
            hay = f"{title} {prof}".lower()

            if q_lower and q_lower not in hay:
                continue

            items.append({
                "pp": str(pp_id),
                "lv": str(lv_id),
                "title": title,
                "lecturers": [p.strip() for p in prof.split("·")] if prof else [],
                "semester": lv.get("semester"),
                "status": lv.get("status"),
                "capacity": lv.get("capacity"),
                "free": lv.get("free"),
                "waitlist": lv.get("waitlist"),
            })
            if limit and len(items) >= limit:
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
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")

    client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
    # open post-login base and try to reach overview
    try:
        client.ensure_overview()
    except Exception as e:
        # If ensure_overview not present because you didn't paste it right:
        raise HTTPException(status_code=500, detail=f"ensure_overview missing: {e}")

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
