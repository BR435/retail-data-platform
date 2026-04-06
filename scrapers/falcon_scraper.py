import time
import random
import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from utils.db import insert_many  # using Supabase REST helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("falcon_scraper")

BASE_URL = "https://www.falcononline.co.uk"

# Persistent session with browser-like headers
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": BASE_URL + "/",
})


def fetch_page(url: str, max_retries: int = 3, backoff: float = 2.0) -> BeautifulSoup:
    """Download a page with retries and return BeautifulSoup object."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching: {url} (attempt {attempt}/{max_retries})")
            resp = session.get(url, timeout=20)
            if resp.status_code == 403:
                logger.warning(f"403 Forbidden for {url}")
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            if attempt == max_retries:
                raise
            sleep_time = backoff * attempt + random.uniform(0, 1)
            logger.info(f"Retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)


def parse_product(product_html) -> Dict:
    """Extract product fields from a product card."""
    # Adjust selectors to match Falcon's actual HTML
    name_el = product_html.select_one(".product-name, .product-title, h3")
    price_el = product_html.select_one(".price, .product-price")
    img_el = product_html.select_one("img")

    if not name_el or not price_el or not img_el:
        raise ValueError("Missing one or more product fields")

    name = name_el.get_text(strip=True)
    price_text = price_el.get_text(strip=True)
    image_url = img_el.get("src", "").strip()

    # Normalise image URL if relative
    if image_url and image_url.startswith("/"):
        image_url = BASE_URL + image_url

    return {
        "name": name,
        "price_raw": price_text,
        "image_url": image_url,
        "source": "falcon",
    }


def scrape_category(category_path: str) -> List[Dict]:
    """Scrape all products from a single category path."""
    url = BASE_URL + category_path
    logger.info(f"Scraping category: {url}")

    soup = fetch_page(url)
    # Adjust selector to match Falcon's product cards
    product_cards = soup.select(".product-card, .product, .product-item")

    products: List[Dict] = []
    logger.info(f"Found {len(product_cards)} product elements in HTML")

    for card in product_cards:
        try:
            product = parse_product(card)
            products.append(product)
        except Exception as e:
            logger.warning(f"Error parsing product: {e}")

    logger.info(f"Parsed {len(products)} products from {url}")
    return products


def scrape_all_categories() -> List[Dict]:
    """Scrape all categories and return list of all products."""
    # TODO: replace with real Falcon category paths
    categories = [
        "/category/fruits",
        "/category/vegetables",
        "/category/snacks",
    ]

    all_products: List[Dict] = []

    for cat in categories:
        try:
            products = scrape_category(cat)
            all_products.extend(products)
        except Exception as e:
            logger.error(f"Category failed ({cat}): {e}")

        # Polite random delay between categories
        sleep_time = random.uniform(1.0, 3.0)
        logger.info(f"Sleeping {sleep_time:.1f}s before next category...")
        time.sleep(sleep_time)

    return all_products


def main():
    logger.info("Starting Falcon scraper...")

    products = scrape_all_categories()

    if not products:
        logger.warning("No products scraped.")
        return {"status": "error", "message": "No products scraped"}

    logger.info(f"Inserting {len(products)} products into Supabase...")

    try:
        response = insert_many("products", products)
        logger.info(f"Insert response: {response}")
    except Exception as e:
        logger.error(f"Failed to insert into Supabase: {e}")
        return {"status": "error", "message": f"Supabase insert failed: {e}"}

    return {
        "status": "success",
        "count": len(products),
        "message": "Falcon scrape completed",
    }


if __name__ == "__main__":
    main()
