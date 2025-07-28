# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List

app = FastAPI()

@app.get("/")
def root():
    return {"greeting": "Hello, World!", "message": "Welcome to FastAPI!"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

class SearchIn(BaseModel):
    username: str
    password: str
    q: str
    limit: Optional[int] = 20

def extract_items(result: Dict[str, Any], q: str, limit: Optional[int]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    data = (result or {}).get("data") or {}
    pp_map = data.get("pp") or {}
    q_lower = (q or "").strip().lower()

    for pp_id, pp_obj in pp_map.items():
        lvs = (pp_obj or {}).get("lvs") or {}
        for lv_id, lv in lvs.items():
            title = (lv or {}).get("name") or ""
            prof  = (lv or {}).get("prof") or ""
            hay   = f"{title} {prof}".lower()
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
    # Lazy import – avoids startup crash if deps are missing
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")
    try:
        return WuLpisApi(user, pw, args=None, sessiondir=None)
    except Exception as e:
        # bubble up login/parse errors
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/courses/search")
def courses_search(p: SearchIn):
    client = get_lpis_client(p.username, p.password)
    res = client.infos()
    if hasattr(client, "getResults"):
        res = client.getResults()
    items = extract_items(res, p.q, p.limit)
    return {"ok": True, "items": items}

# main.py — add:
from fastapi import HTTPException

@app.post("/debug/structure")
def debug_structure(p: SearchIn):
    try:
        from lpislib import WuLpisApi
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LPIS client not available: {e}")
    client = WuLpisApi(p.username, p.password, args=None, sessiondir=None)
    data = client.infos()
    pp = (data or {}).get("pp") or {}
    total_lvs = sum(len((pp[k].get("lvs") or {})) for k in pp)
    return {
        "ok": True,
        "studies_count": (data or {}).get("studies_count"),
        "pp_count": len(pp),
        "lv_total": total_lvs,
        "sample_pp_ids": list(pp.keys())[:5],
    }
