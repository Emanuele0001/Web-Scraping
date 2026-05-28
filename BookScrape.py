import asyncio
import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import logging
import psutil
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from typing import Optional
 
# ketu thjeshte eshte bere konfig i siteve 
SITES = {
    "books_toscrape": {
        "base_url": "https://books.toscrape.com/catalogue/page-{}.html",
        "parser": "books_toscrape",
        "max_pages": 50,
    },
    "quotes_toscrape": {
        "base_url": "https://quotes.toscrape.com/page/{}/",
        "parser": "quotes_toscrape",
        "max_pages": 10,
    },
}
 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
 
RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
 
#Loging
logging.basicConfig(
    filename="multi_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# dataclasses per te strukturuar te dhenat

@dataclass
class Book:
    source: str
    title: str
    price: str
    rating: int
    availability: str
 
@dataclass
class Quote:
    source: str
    text: str
    author: str
    tags: str  
 
 
#  nje function parser per secilin site 

 
def parse_books_toscrape(html: str, source: str) -> list[Book]:
    """Parser perr books.toscrape.com — paradigma: OOP + functional list processing"""
    soup = BeautifulSoup(html, "html.parser")
    books = soup.find_all("article", class_="product_pod")
    
    result = []
    for book in books:
        title = book.h3.a["title"]
        price = book.find("p", class_="price_color").text.strip()
        rating_class = book.find("p", class_="star-rating")["class"][1]
        rating = RATING_MAP.get(rating_class, 0)
        availability = book.find("p", class_="instock")
        availability = availability.text.strip() if availability else "Unknown"
    
        result.append(Book(source=source, title=title, price=price,
                           rating=rating, availability=availability))
        
    return result
 
 
def parse_quotes_toscrape(html: str, source: str) -> list[Quote]:
    """Parser per quotes.toscrape.com — paradigma: OOP + functional list processing"""
    soup = BeautifulSoup(html, "html.parser")
    quote_divs = soup.find_all("div", class_="quote")
    result = []
    for q in quote_divs:
        text = q.find("span", class_="text").text.strip()
        author = q.find("small", class_="author").text.strip()
        tags = "|".join(tag.text.strip() for tag in q.find_all("a", class_="tag"))
        
        result.append(Quote(source=source, text=text, author=author, tags=tags))
        
    return result
 
 
# rooteri emrir i pareserit
PARSERS = {
    "books_toscrape": parse_books_toscrape,
    "quotes_toscrape": parse_quotes_toscrape,
}
 
 
# fetching  paradigma sinkrone requests brenda thread-it
 
def fetch_page_sync(site_name: str, page_number: int, retries: int = 3) -> Optional[list]:
    """
    ✅ requests (sinkron) — ekzekutohet brenda ThreadPoolExecutor.
    Kthen:
      - listë me objekte nëse ka sukses
      - listë bosh nëse gabim i rikuperueshëm
      - None  nëse faqja nuk ekziston (404) → sinjal për fund
    """
    site = SITES[site_name]
    url = site["base_url"].format(page_number)
    parser_fn = PARSERS[site["parser"]]
 
 
 
 
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
 
            if response.status_code == 404:
                logging.warning(f"[{site_name}] Page {page_number} → 404, fund i katalogut.")
                return None
 
            if response.status_code != 200:
                logging.warning(f"[{site_name}] Page {page_number} → status {response.status_code}")
                return []
 
            items = parser_fn(response.text, site_name)
            logging.info(f"[{site_name}] Faqja {page_number} — {len(items)} artikuj")
            return items
 
        except requests.exceptions.RequestException as e:
            logging.error(f"[{site_name}] Retry {attempt + 1}/faqja {page_number}: {e}")
            time.sleep(1)
 
    return []
 
 

 
async def fetch_page_async(executor: ThreadPoolExecutor, site_name: str, page_number: int):
 
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, fetch_page_sync, site_name, page_number)
 
 
async def scrape_site(executor: ThreadPoolExecutor, site_name: str) -> list:
    """
    Scrape një site të plotë duke procesuar BATCH_SIZE faqe njëkohësisht.
    Ndalon kur merr None (404) ose arrin max_pages.
    """
    BATCH_SIZE = 5
    max_pages = SITES[site_name]["max_pages"]
    all_items = []
    page = 1
 
    while page <= max_pages:
        #  asyncio.gather  ekzekutim konkurrent i batch-it
        batch = [
            fetch_page_async(executor, site_name, page + i)
            for i in range(BATCH_SIZE)
            if (page + i) <= max_pages
        ]
        results = await asyncio.gather(*batch)
 
        ended = False
        for result in results:
            if result is None:
                ended = True
                break
            all_items.extend(result)
            if result:
                print(f"  [{site_name}] +{len(result)} artikuj (faqja ~{page})")
 
        if ended:
            print(f"  [{site_name}] Fund i katalogut.")
            break
 
        page += BATCH_SIZE
 
    return all_items
 
 
#outputi  CSV + JSON per çdo site + skedar i kombinuar
 
def save_results(all_data: dict[str, list]):
    """Ruan të dhënat për secilin site dhe një skedar të kombinuar JSON."""
 
#Per site CSV dhe JSon
    for site_name, items in all_data.items():
        if not items:
            continue
 
        rows = [asdict(item) for item in items]
 
        csv_file = f"{site_name}.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {csv_file} — {len(rows)} rreshta")
 
        json_file = f"{site_name}.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"  {json_file}")
 
 
    #  skedar i kombinuar të gjitha site-t bashkë
    combined = []
    for items in all_data.values():
        combined.extend([asdict(item) for item in items])
 
    with open("all_sites_combined.json", "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    print(f"  all_sites_combined.json — {len(combined)} artikuj total")
 
 

 
async def main():
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss / 1024 / 1024
    start_time = time.time()
 
    print(" Duke nisur multi-site scraper...\n")
 
    all_data: dict[str, list] = {}
 
    with ThreadPoolExecutor(max_workers=10) as executor:


        site_names = list(SITES.keys())
        results = await asyncio.gather(
            *[scrape_site(executor, site_name) for site_name in site_names]
        )
 
    for site_name, items in zip(site_names, results):
        all_data[site_name] = items
        print(f"\n {site_name}: {len(items)} artikuj total")
 
    print("\n Duke ruajtur skedarët...")
    save_results(all_data)
 
    end_time = time.time()
    end_memory = process.memory_info().rss / 1024 / 1024
 
    print(f"\n{'─'*45}")
    print(f" Artikuj total:      {sum(len(v) for v in all_data.values())}")
    print(f"⏱  Kohë ekzekutimi:   {end_time - start_time:.2f} sek")
    print(f" Memorie e përdorur: {end_memory - start_memory:.2f} MB")
    print(f"{'─'*45}")
 
 
if __name__ == "__main__":
    asyncio.run(main())
