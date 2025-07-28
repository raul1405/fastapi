from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any, List

app = FastAPI()

class SearchIn(BaseModel):
    username: str
    password: str
    q: str
    limit: Optional[int] = 20

def make_lpis_client(user: str, pw: str):
    """
    Versucht, die WuLpisApi aus verschiedenen Modulpfaden zu importieren –
    je nach Version heißt die Klasse/Datei leicht anders.
    """
    try:
        from wu_lpis_api.WuLpisApiClass import WuLpisApi  # häufige Variante
        return WuLpisApi(user, pw, args=None, sessiondir=None)
    except Exception:
        try:
            from wu_lpis_api import WuLpisApi
            return WuLpisApi(user, pw, args=None, sessiondir=None)
        except Exception as e:
            raise RuntimeError(f"wu-lpis-api not importable: {e}")

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

@app.post("/courses/search")
def courses_search(p: SearchIn):
    """
    Loggt sich ins LPIS ein, ruft infos() und filtert LVs nach Suchbegriff.
    Keine Demo-Daten mehr.
    """
    try:
        if not p.username or not p.password:
            raise HTTPException(status_code=400, detail="Missing credentials")

        client = make_lpis_client(p.username, p.password)

        # je nach Bibliotheksversion:
        res = client.infos()
        if hasattr(client, "getResults"):
            res = client.getResults()

        items = extract_items(res, p.q, p.limit)
        return {"ok": True, "items": items}
    except HTTPException:
        raise
    except Exception as e:
        # 502, damit Frontend klar 'Worker/LPIS Fehler' unterscheiden kann
        raise HTTPException(status_code=502, detail=str(e))

@app.get("/healthz")
def healthz():
    return {"ok": True}
