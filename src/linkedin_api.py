import os, time, requests
from datetime import datetime
from dateutil.relativedelta import relativedelta  # pip install python-dateutil

BASE = "https://api.linkedin.com/rest"

class LIError(RuntimeError): ...
def _candidate_versions_back(months_back: int = 24):
    vers, d = [], datetime.utcnow().replace(day=1)
    for _ in range(months_back):
        vers.append(d.strftime("%Y%m"))
        d = d - relativedelta(months=1)
    return vers

def _env_token(kind: str) -> str:
    # kind: "cm" | "pages" | "ads"
    env_map = {
        "cm": "LI_CM_ACCESS_TOKEN",
        "pages": "LI_PAGES_ACCESS_TOKEN",
        "ads": "LI_ADS_ACCESS_TOKEN",
    }
    var = env_map.get(kind)
    return os.getenv(var or "", "")

def li_get(path: str, params: dict | None = None, *, kind: str = "cm",
           token: str | None = None, version: str | None = None) -> dict:
    tok = token or _env_token(kind)
    if not tok:
        raise LIError(f"Falta access token para kind='{kind}'. Revisa tu .env")

    url = f"{BASE}/{path}"
    last_426 = None
    versions = [version] if version else _candidate_versions_back(24)

    for ver in versions:
        hdrs = {
            "Authorization": f"Bearer {tok}",
            "Accept": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": ver,  # YYYYMM
        }
        resp = requests.get(url, headers=hdrs, params=params or {})
        if resp.status_code == 426:
            last_426 = resp.text
            continue
        if resp.status_code >= 400:
            raise LIError(f"LinkedIn {resp.status_code}: {resp.text[:400]}")
        return resp.json()

    raise LIError(
        f"LinkedIn 426: ninguna versión reciente activa. Última resp: {last_426[:300] if last_426 else ''}"
    )

def paginate_elements(path: str, params: dict | None = None, *,
                      count: int = 100, kind: str = "cm",
                      token: str | None = None, version: str | None = None):
    start = 0
    while True:
        q = dict(params or {})
        q.update({"start": start, "count": count})
        js = li_get(path, q, kind=kind, token=token, version=version)
        items = js.get("elements") or []
        for it in items:
            yield it
        paging = js.get("paging") or {}
        total = paging.get("total")
        if total is None:
            if not items:
                break
        if total is not None and start + count >= int(total):
            break
        start += count
        time.sleep(0.2)
