# src/li_discover_ids.py
import os
import requests
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()
ACCESS_TOKEN = os.getenv("LI_ACCESS_TOKEN")  # agrega esta variable en tu .env

if not ACCESS_TOKEN:
    raise RuntimeError("Falta LI_ACCESS_TOKEN en el .env")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

def get_organizations():
    """Obtiene las páginas de empresa (organizations) asociadas al usuario."""
    url = "https://api.linkedin.com/v2/organizationAcls?q=roleAssignee&role=ADMINISTRATOR&state=APPROVED"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    orgs = []
    for e in data.get("elements", []):
        org = e.get("organization")
        if org:
            org_id = org.split(":")[-1]
            orgs.append(org_id)
    return orgs

def get_ad_accounts():
    """Obtiene las cuentas publicitarias (ad accounts) del usuario."""
    url = "https://api.linkedin.com/v2/adAccountsV2?q=search"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    data = r.json()

    accounts = []
    for e in data.get("elements", []):
        acc_id = e.get("id")
        if isinstance(acc_id, int):
            accounts.append(str(acc_id))
        elif isinstance(acc_id, str):
            accounts.append(acc_id.split(":")[-1])
    return accounts


def main():
    print("🔹 Buscando organizaciones (LI_ORG_ID)...")
    orgs = get_organizations()
    for o in orgs:
        print(f"  → LI_ORG_ID = {o}")

    print("\n🔹 Buscando cuentas publicitarias (LI_AD_ACCOUNT_ID)...")
    ads = get_ad_accounts()
    for a in ads:
        print(f"  → LI_AD_ACCOUNT_ID = {a}")

if __name__ == "__main__":
    main()
