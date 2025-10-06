from li_api import li_get
import os
from dotenv import load_dotenv

load_dotenv()
LI_ORG_ID = os.getenv("LI_ORG_ID")

url = "https://api.linkedin.com/rest/shares"
params = {
    "q": "owners",
    "owners": f"urn:li:organization:{LI_ORG_ID}",
    "sharesPerOwner": 10
}
data = li_get(url, params=params, mode="shares")   # <-- usar HEADERS_SHARES



print("🔹 Probando shares...")
try:
    js = li_get(url, params=params, mode="shares")
    for e in js.get("elements", []):
        print(e.get("id"), e.get("permalink"))
except Exception as ex:
    print("⚠ Error:", ex)

from li_api import HEADERS_DMA, HEADERS_ADS, HEADERS_SHARES  

print("HEADERS ADS:", HEADERS_ADS)
print("HEADERS DMA:", HEADERS_DMA)
print("HEADERS_SHARES:",HEADERS_SHARES)

