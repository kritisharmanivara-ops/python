import requests
from bs4 import BeautifulSoup
import csv
import re
from urllib.parse import urljoin

BASE_URL = "https://trnk-nyc.com"
START_URL = BASE_URL
visited = set()
products = []

PRODUCT_LIMIT = 10 


def slugify(text):
    text = text.lower().strip()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text.strip("-")


def extract_price(text):
    if not text:
        return ""
    match = re.search(r"\d+(?:\.\d{1,2})?", text)
    return match.group(0) if match else ""


def fetch(url):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        else:
            print(f"Failed to fetch {url}, status code: {r.status_code}")
            return None
    except Exception as e:
        print(f"Fetch error: {e} on {url}")
        return None


def extract_product(url):
    if len(products) >= PRODUCT_LIMIT or url in visited:
        return
    visited.add(url)

    soup = fetch(url)
    if not soup:
        return

    # Product title
    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""
    if not title:
        return
    handle = slugify(title)

    # Description HTML (grab main product description)
    desc_tag = soup.find("div", {"class": "product-description"})
    description = str(desc_tag) if desc_tag else ""

    # All product images (absolute URLs)
    images = []
    # Shopify product images usually in div with class 'product-single__photo' or similar
    # Try to find images with /products/ in src
    for img in soup.select("img"):
        src = img.get("src") or ""
        if src and "/products/" in src:
            full_img = urljoin(BASE_URL, src)
            if full_img not in images:
                images.append(full_img)

    if not images:
        # Fallback: find first img inside product main section
        main_img = soup.select_one("img")
        if main_img:
            images.append(urljoin(BASE_URL, main_img.get("src", "")))

    # Variant names and values
    option_name_tags = soup.select(".single-option-selector")
    option_names = [tag.get("data-option-name") for tag in option_name_tags if tag.get("data-option-name")]

    # Fallback option names if none found
    if not option_names:
        option_names = ["Title"]

    # Shopify variant options selector usually in select[name='id'] option
    variant_options = soup.select("select[name='id'] option")

    if variant_options:
        for v in variant_options:
            if len(products) >= PRODUCT_LIMIT:
                break

            option_value = v.get_text(strip=True)
            option_values = [ov.strip() for ov in option_value.split('/')]

            # Handle >3 options by merging extras
            if len(option_values) > 3:
                merged_value = " / ".join(option_values[2:])
                option_values = option_values[:2] + [merged_value]

            variant_price_text = v.get("data-price") or ""
            variant_price = extract_price(variant_price_text)

            variant_sku = v.get("data-sku") or ""
            inventory_quantity = v.get("data-inventory-quantity") or ""
            inventory_policy = "deny"
            requires_shipping = "TRUE"
            taxable = "TRUE"

            row = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": description,
                "Vendor": "",
                "Type": "",
                "Tags": "",
                "Published": "TRUE",
                "Option1 Name": option_names[0] if len(option_names) > 0 else "",
                "Option1 Value": option_values[0] if len(option_values) > 0 else "",
                "Option2 Name": option_names[1] if len(option_names) > 1 else "",
                "Option2 Value": option_values[1] if len(option_values) > 1 else "",
                "Option3 Name": option_names[2] if len(option_names) > 2 else "",
                "Option3 Value": option_values[2] if len(option_values) > 2 else "",
                "Variant SKU": variant_sku,
                "Variant Price": variant_price,
                "Variant Inventory Qty": inventory_quantity,
                "Variant Inventory Policy": inventory_policy,
                "Variant Requires Shipping": requires_shipping,
                "Variant Taxable": taxable,
                "Image Src": images[0] if images else "",
                "Image Position": 1,
                "Image Alt Text": title
            }
            products.append(row)

        # Add additional images as separate rows (without variant data)
        if len(images) > 1:
            for i, img_url in enumerate(images[1:], start=2):
                if len(products) >= PRODUCT_LIMIT:
                    break
                row = {
                    "Handle": handle,
                    "Title": title,
                    "Body (HTML)": description,
                    "Vendor": "",
                    "Type": "",
                    "Tags": "",
                    "Published": "TRUE",
                    "Option1 Name": "",
                    "Option1 Value": "",
                    "Option2 Name": "",
                    "Option2 Value": "",
                    "Option3 Name": "",
                    "Option3 Value": "",
                    "Variant SKU": "",
                    "Variant Price": "",
                    "Variant Inventory Qty": "",
                    "Variant Inventory Policy": "",
                    "Variant Requires Shipping": "",
                    "Variant Taxable": "",
                    "Image Src": img_url,
                    "Image Position": i,
                    "Image Alt Text": title
                }
                products.append(row)

    else:
        # No variants found, single default variant
        price_tag = soup.select_one("[data-product-price], .price, .product-price")
        price_text = price_tag.get_text() if price_tag else ""
        price = extract_price(price_text)

        row = {
            "Handle": handle,
            "Title": title,
            "Body (HTML)": description,
            "Vendor": "",
            "Type": "",
            "Tags": "",
            "Published": "TRUE",
            "Option1 Name": "Title",
            "Option1 Value": "Default Title",
            "Variant SKU": "",
            "Variant Price": price,
            "Variant Inventory Qty": "",
            "Variant Inventory Policy": "deny",
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
            "Image Src": images[0] if images else "",
            "Image Position": 1,
            "Image Alt Text": title
        }
        products.append(row)

    print("✔ Scraped:", title)


def crawl(url):
    if len(products) >= PRODUCT_LIMIT or url in visited:
        return
    visited.add(url)

    soup = fetch(url)
    if not soup:
        return

    for link in soup.find_all("a", href=True):
        if len(products) >= PRODUCT_LIMIT:
            break

        href = link["href"]
        full_url = urljoin(BASE_URL, href)

        # Crawl only internal links
        if not full_url.startswith(BASE_URL):
            continue

        # If product page found, extract
        if "/products/" in full_url:
            extract_product(full_url)
        else:
            # Else crawl deeper
            crawl(full_url)


if __name__ == "__main__":
    print("🔍 Crawling started...")
    crawl(START_URL)

    fieldnames = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
        "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value", "Option3 Name", "Option3 Value",
        "Variant SKU", "Variant Price", "Variant Inventory Qty", "Variant Inventory Policy",
        "Variant Requires Shipping", "Variant Taxable",
        "Image Src", "Image Position", "Image Alt Text"
    ]

    with open("shopify_products.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(products)

    print(f"📁 CSV saved as shopify_products.csv")
    print(f"Total products scraped: {len(products)}")
