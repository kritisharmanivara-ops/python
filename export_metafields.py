import requests
import json

# ================= CONFIG =================
STORE_A = "private-app-2.myshopify.com"
ACCESS_TOKEN_A = "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"
API_VERSION = "2024-07"
EXPORT_FILE = "metafields_export.json"
# ==========================================

url = f"https://{STORE_A}/admin/api/{API_VERSION}/graphql.json"

headers = {
    "X-Shopify-Access-Token": ACCESS_TOKEN_A,
    "Content-Type": "application/json"
}

query = """
{
  metafieldDefinitions(ownerType: PRODUCT, first: 250) {
    edges {
      node {
        id
        name
        namespace
        key
        type {
          name
        }
        description
        ownerType
      }
    }
  }
}
"""

def main():
    response = requests.post(url, headers=headers, json={"query": query})

    if response.status_code != 200:
        print(f"[ERROR] Failed: {response.status_code} {response.text}")
        return

    data = response.json()
    metafields = data["data"]["metafieldDefinitions"]["edges"]

    print(f"[INFO] Total metafield definitions exported: {len(metafields)}")

    with open(EXPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(metafields, f, indent=2)


if __name__ == "__main__":
    main()
