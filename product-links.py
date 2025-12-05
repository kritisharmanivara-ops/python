import requests
from bs4 import BeautifulSoup
import csv
from urllib.parse import urljoin

visited = set()
products = []

BASE_URL = "https://trnk-nyc.com/"
START_URL = BASE_URL

def fetch(url):
    try:
        r = requests.get(url, timeout=10)
        return BeautifulSoup(r.text, "html.parser")
    except:
        return None

def extract_product(url):
    soup = fetch(url)
    if not soup: 
        return
    
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else ""
    price = soup.find(class_="price").get_text(strip=True) if soup.find(class_="price") else ""
    image = soup.find("img")["src"] if soup.find("img") else ""

    products.append({
        "url": url,
        "title": title,
        "price": price,
        "image": image
    })

    print("✔ scraped:", title)

def crawl(url):
    if url in visited:
        return
    visited.add(url)

    soup = fetch(url)
    if not soup:
        return
    
    # find all links
    for link in soup.find_all("a", href=True):
        href = link["href"]
        full = urljoin(BASE_URL, href)

        # detect product page
        if "/product" in full or "/products" in full:
            extract_product(full)
        
        # continue crawl only inside site
        if BASE_URL in full:
            crawl(full)

print("🔍 crawling started...")
crawl(START_URL)

# save csv
with open("products.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["url","title","price","image"])
    writer.writeheader()
    writer.writerows(products)

print("📁 CSV Generated: products.csv")
print("Total Products:", len(products))
