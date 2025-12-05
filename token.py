import requests
import pandas as pd

# --- CONFIG ---
SHOPIFY_STORE = "your-store.myshopify.com"     # Your store domain
ACCESS_TOKEN = "your_admin_api_token_here"    # Your Admin API access token
CHECK_FILE = "check_products.xlsx"             # Your file with URLs/handles
OUTPUT_FILE = "shopify_link_check_results.csv"

# --- SETUP HEADERS FOR API ---
HEADERS = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# --- FUNCTIONS TO CHECK PRODUCT AND COLLECTION ---
def get_product_by_handle(handle):
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-07/products.json?handle={handle}"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    if "products" in data and len(data["products"]) > 0:
        return data["products"][0]
    return None

def get_collection_by_handle(handle):
    # Check custom collections
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-07/custom_collections.json?handle={handle}"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    if "custom_collections" in data and len(data["custom_collections"]) > 0:
        return data["custom_collections"][0]
    # Check smart collections if needed
    url2 = f"https://{SHOPIFY_STORE}/admin/api/2024-07/smart_collections.json?handle={handle}"
    response2 = requests.get(url2, headers=HEADERS)
    data2 = response2.json()
    if "smart_collections" in data2 and len(data2["smart_collections"]) > 0:
        return data2["smart_collections"][0]
    return None

# --- HELPER TO EXTRACT HANDLE ---
def extract_handle(url):
    if not isinstance(url, str):
        return ""
    url = url.strip()
    if "/products/" in url:
        return url.split("/products/")[1].split("?")[0].split("/")[0]
    if "/collections/" in url:
        return url.split("/collections/")[1].split("?")[0].split("/")[0]
    return url.strip()

# --- LOAD CHECK FILE ---
df = pd.read_excel(CHECK_FILE)  # Or pd.read_csv()

results = []

for idx, row in df.iterrows():
    url_or_handle = row['handle_or_url']
    handle = extract_handle(url_or_handle)

    if "/collections/" in str(url_or_handle):
        # Check collection
        collection = get_collection_by_handle(handle)
        if collection:
            published = collection.get("published_at") is not None
            reason = "" if published else "Collection is unpublished → 404"
            results.append({
                "url": url_or_handle,
                "type": "Collection",
                "handle": handle,
                "exists": "Yes",
                "published": "Yes" if published else "No",
                "reason_404": reason
            })
        else:
            results.append({
                "url": url_or_handle,
                "type": "Collection",
                "handle": handle,
                "exists": "No",
                "published": "No",
                "reason_404": "Collection not found"
            })
    else:
        # Check product
        product = get_product_by_handle(handle)
        if product:
            status = product.get("status", "active")  # active, draft, archived
            published = product.get("published_at") is not None
            reason = ""
            if status != "active":
                reason = f"Product status is {status} → 404"
            elif not published:
                reason = "Product is unpublished → 404"
            results.append({
                "url": url_or_handle,
                "type": "Product",
                "handle": handle,
                "exists": "Yes",
                "status": status,
                "published": "Yes" if published else "No",
                "reason_404": reason
            })
        else:
            results.append({
                "url": url_or_handle,
                "type": "Product",
                "handle": handle,
                "exists": "No",
                "status": "N/A",
                "published": "No",
                "reason_404": "Product not found"
            })

# --- SAVE RESULTS ---
pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
print("Check completed. Results saved to:", OUTPUT_FILE)
