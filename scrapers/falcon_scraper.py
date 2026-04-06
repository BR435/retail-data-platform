import requests
from bs4 import BeautifulSoup
from utils.db import insert_row, insert_many, upsert_row
import time

BASE_URL = "https://www.falcononline.co.uk"

def fetch_page(url):
    """Download a page and return BeautifulSoup object."""
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def parse_product(product_html):
    """Extract product fields from a product card."""
    name = product_html.select_one(".product-name").get_text(strip=True)
    price = product_html.select_one(".price").get_text(strip=True)
    image = product_html.select_one("img")["src"]

    return {
        "name": name,
        "price": price,
        "image_url": image,
        "source": "falcon",
    }

def scrape_category(category_url):
    """Scrape all products from a category page."""
    soup = fetch_page(category_url)
    product_cards = soup.select(".product-card")

    products = []
    for card in product_cards:
        try:
            product = parse_product(card)
            products.append(product)
        except Exception as e:
            print("Error parsing product:", e)

    return products

def scrape_all_categories():
    """Scrape all categories and return list of all products."""
    categories = [
        "/category/fruits",
        "/category/vegetables",
        "/category/snacks",
        # Add more categories here
    ]

    all_products = []

    for cat in categories:
        url = BASE_URL + cat
        print("Scraping:", url)

        try:
            products = scrape_category(url)
            all_products.extend(products)
        except Exception as e:
            print("Category failed:", e)

        time.sleep(1)  # be polite

    return all_products

def main():
    print("Starting Falcon scraper...")

    products = scrape_all_categories()

    if not products:
        print("No products scraped.")
        return {"status": "error", "message": "No products scraped"}

    print(f"Inserting {len(products)} products into Supabase...")

    # Bulk insert into Supabase
    response = insert_many("products", products)

    print("Insert response:", response)

    return {
        "status": "success",
        "count": len(products),
        "message": "Falcon scrape completed"
    }

if __name__ == "__main__":
    main()
