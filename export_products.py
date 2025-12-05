import requests
import json

STORE_A_DOMAIN = "private-app-2.myshopify.com"
STORE_A_TOKEN = "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"
API_VERSION = "2024-07"

def shopify_get(url):
    headers = {"X-Shopify-Access-Token": STORE_A_TOKEN}
    return requests.get(url, headers=headers).json()

def export_products():
    all_data = []
    url = f"https://{STORE_A_DOMAIN}/admin/api/{API_VERSION}/products.json?limit=250"

    products = shopify_get(url)["products"]

    for p in products:
        pid = p["id"]

        metafields_url = f"https://{STORE_A_DOMAIN}/admin/api/{API_VERSION}/products/{pid}/metafields.json"
        metafields = shopify_get(metafields_url).get("metafields", [])

        all_data.append({
            "product": p,
            "metafields": metafields
        })

    with open("products_export.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=4)

    print(f"Export complete: {len(all_data)} products saved to products_export.json")


if __name__ == "__main__":
    export_products()
