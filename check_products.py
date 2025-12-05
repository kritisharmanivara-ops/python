import pandas as pd
import os

# ========= INPUT FILES =========
SHOPIFY_CSV_1 = "products_export.csv"
SHOPIFY_CSV_2 = "products_export_2.csv"
CHECK_FILE     = "check_products.xlsx"
OUTPUT_CSV = "checked_results_new.csv"


# ========= READ SHOPIFY FILES SAFELY =========
p1 = pd.read_csv(SHOPIFY_CSV_1, encoding="latin1", low_memory=False)
p2 = pd.read_csv(SHOPIFY_CSV_2, encoding="latin1", low_memory=False)

# Combine both CSVs
products = pd.concat([p1, p2], ignore_index=True)

# ========= PREPARE PRODUCT DATA =========
products["handle"] = products["Handle"].astype(str).str.strip()
products["status_lower"] = products["Status"].astype(str).str.lower()
products["published_str"] = products["Published"].astype(str).str.upper()

# ========= READ CHECK FILE =========
if CHECK_FILE.endswith(".xlsx"):
    checks = pd.read_excel(CHECK_FILE)
else:
    checks = pd.read_csv(CHECK_FILE, encoding="latin1", low_memory=False)

# ========= Extract handle from URL or collection URL =========
def extract_handle(url):
    if not isinstance(url, str):
        return ""

    url = url.strip()

    # /products/handle
    if "/products/" in url:
        return url.split("/products/")[1].split("?")[0].split("/")[0]

    # /collections/collection-name
    if "/collections/" in url:
        return url.split("/collections/")[1].split("?")[0].split("/")[0]

    return url.strip()

checks["handle"] = checks["handle_or_url"].apply(extract_handle)

# ========= Detect collection link =========
def detect_collection(url):
    return "/collections/" in str(url)

checks["is_collection"] = checks["handle_or_url"].apply(detect_collection)

# ========= MAIN PROCESSING =========
results = []

for _, row in checks.iterrows():

    url = row["handle_or_url"]
    handle = row["handle"]
    is_collection = row["is_collection"]

    # Match product
    product_match = products[products["handle"] == handle]

    # ---- Not Found ----
    if product_match.empty:
        results.append({
            "url": url,
            "handle": handle,
            "product_status": "NOT FOUND",
            "sales_channel_assigned": "No",
            "404_reason": "Product not found in Shopify CSV"
        })
        continue

    product = product_match.iloc[0]

    # Product status
    status = product["status_lower"]

    # ANY sales channel enabled?
    sales_channel_assigned = "Yes" if "TRUE" in product["published_str"] else "No"

    # ========= Determine reason for 404 =========
    if status == "draft":
        reason = "Product is Draft -> shows 404"
    elif status == "archived":
        reason = "Product is Archived -> shows 404"
    elif sales_channel_assigned == "No":
        reason = "No sales channel assigned -> product not visible"
    else:
        reason = ""

    # Collection URL extra info
    if is_collection:
        if not reason:
            reason = "Collection URL used -> not a product page"
        in_collection = "Unknown (need collection export)"
    else:
        in_collection = "Not a collection link"

    # Append result
    results.append({
        "url": url,
        "handle": handle,
        "product_status": product["Status"],
        "sales_channel_assigned": sales_channel_assigned,
        "404_reason": reason
    })

# ========= SAVE OUTPUT =========
pd.DataFrame(results).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print("DONE — Output saved to:", OUTPUT_CSV)
