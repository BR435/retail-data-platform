import time
import re
import requests
from bs4 import BeautifulSoup
from utils.db import get_db_conn

def extract_brand_size(name):
	parts = name.split()

	# Brand = first word (e.g. Heera, TRS, East End)
	brand = parts[0] if parts else None

	# Size pattern: 2kg, 500g, 1.5 L, 6pcs, etc.
	size_pattern = r"(\d+(\.\d+)?\s?(kg|g|ml|l|L|pcs|pc|pack|pk))"
	match = re.search(size_pattern, name, re.IGNORECASE)
	size = match.group(0) if match else None

	return brand, size

# ----------------------------------------------------
# FETCH HTML WITH RETRIES + BACKOFF
# ----------------------------------------------------
session = requests.Session()

def fetch_html(url, retries=5):
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
	}

	for attempt in range(1, retries + 1):
		try:
			response = session.get(url, timeout=40, headers=headers)
			response.raise_for_status()
			return response.text

		except (requests.exceptions.ReadTimeout,
				requests.exceptions.ConnectionError) as e:
			print(f"⚠️ Error fetching {url}: {e} (attempt {attempt}/{retries})")
			sleep_time = attempt * 2
			print(f"⏳ Retrying in {sleep_time}s...")
			time.sleep(sleep_time)

			if attempt == retries:
				raise


# ----------------------------------------------------
# SAVE CATEGORY
# ----------------------------------------------------
def save_category(conn, store_id, name, url):
	cur = conn.cursor()
	cur.execute("""
		INSERT INTO categories (store_id, category_name, category_url)
		VALUES (%s, %s, %s)
		ON CONFLICT (store_id, category_url) DO UPDATE
		SET category_name = excluded.category_name
		RETURNING id;
	""", (store_id, name, url))
	category_id = cur.fetchone()[0]
	conn.commit()
	return category_id


# ----------------------------------------------------
# PRODUCT DETECTION
# ----------------------------------------------------
def page_has_products(soup):
	return bool(
		soup.select("a.product-item-link") or
		soup.select("div.product-item-info") or
		soup.select("strong.product.name a")
	)


# ----------------------------------------------------
# PRODUCT SCRAPER
# ----------------------------------------------------
def scrape_products(conn, store_id, category_id, base_url):
	page = 1

	while True:
		paged_url = f"{base_url}?p={page}"
		print(f"{'  ' * 3}Scraping page {page} → {paged_url}")

		html = fetch_html(paged_url)
		soup = BeautifulSoup(html, "html.parser")

		product_cards = soup.select("a.product-item-link")

		# Stop when no more products
		if not product_cards:
			print(f"{'  ' * 3}No products found on page {page}. Stopping pagination.")
			break

		for link in product_cards:
			name = link.get_text(strip=True)
			product_url = link.get("href")

			price_tag = link.find_next("span", class_="price")
			price_text = price_tag.get_text(strip=True) if price_tag else None

			if price_text:
				price_clean = price_text.replace("£", "").strip()
				try:
					price_pence = int(float(price_clean) * 100)
				except:
					price_pence = None
			else:
				price_pence = None
				
			brand, size = extract_brand_size(name)	

			product_id = save_product(
				conn,
				store_id,
				category_id,
				name,
				brand,
				size,
				product_url
			)

			if price_pence is not None:
				save_price(conn, product_id, price_pence)

			print(f"{'  ' * 4}Saved product: {name}")

		page += 1
		
# ----------------------------------------------------
# SUBCATEGORY SCRAPER (FULLY FIXED)
# ----------------------------------------------------
def scrape_subcategories(conn, store_id, parent_id, parent_url, level):
	html = fetch_html(parent_url)
	soup = BeautifulSoup(html, "html.parser")

	# If this page has products, scrape them and stop
	if page_has_products(soup):
		scrape_products(conn, store_id, parent_id, parent_url)

		cur = conn.cursor()
		cur.execute("UPDATE categories SET scraped = TRUE WHERE id = %s", (parent_id,))
		conn.commit()
		return

	# Look for subcategories
	subcats = soup.select("a.title-cat-mega-menu")

	# Fallback for deeper levels
	if not subcats:
		subcats = soup.select("div.category-item a")

	if not subcats:
		return

	seen = set()

	for link in subcats:
		name = link.get_text(strip=True)
		raw_url = link.get("href")

		print("DEBUG RAW URL:", repr(raw_url))

		# Normalize URL safely
		url = (raw_url or "").strip()

		# Skip clearly invalid URLs
		if (
			not url or
			"javascript" in url.lower() or
			"void" in url.lower() or
			url in {"#", ""}
		):
			print(f"Skipping invalid link: {url}")
			continue

		# Normalize
		url = url.rstrip("/")
		parent_url_clean = parent_url.rstrip("/")

		# Skip self-links
		if url == parent_url_clean:
			continue

		if not name:
			continue

		# Prevent duplicates by name at this level
		if name in seen:
			continue
		seen.add(name)

		# FINAL GUARD BEFORE SAVING:
		# Only save categories with real HTTP(S) URLs
		if not (url.startswith("http://") or url.startswith("https://")):
			print(f"Skipping non-HTTP category (not saved): {url}")
			continue

		# Save subcategory
		subcat_id = save_category(conn, store_id, name, url)

		# Skip if same category ID (Magento bug)
		if subcat_id == parent_id:
			continue

		# Assign parent + level
		cur = conn.cursor()
		cur.execute("""
			UPDATE categories
			SET parent_id = %s, level = %s
			WHERE id = %s
		""", (parent_id, level, subcat_id))
		conn.commit()

		print(f"{'  ' * level}Saved subcategory: {name} (ID {subcat_id})")

		# Recurse deeper (only real HTTP URLs reach here)
		scrape_subcategories(conn, store_id, subcat_id, url, level + 1)


# ----------------------------------------------------
# PRODUCT + PRICE SAVE
# ----------------------------------------------------
def save_product(conn, store_id, category_id, name, brand, size, url):
	cur = conn.cursor()
	cur.execute("""
		INSERT INTO products (store_id, category_id, product_name, brand, size, product_url)
		VALUES (%s, %s, %s, %s, %s, %s)
		ON CONFLICT (store_id, product_url) DO UPDATE
		SET product_name = excluded.product_name,
			brand = excluded.brand,
			size = excluded.size,
			category_id = excluded.category_id
		RETURNING id;
	""", (store_id, category_id, name, brand, size, url))
	product_id = cur.fetchone()[0]
	conn.commit()
	return product_id


def save_price(conn, product_id, price_pence):
	cur = conn.cursor()
	cur.execute("""
		INSERT INTO prices (product_id, price_pence)
		VALUES (%s, %s);
	""", (product_id, price_pence))
	conn.commit()


# ----------------------------------------------------
# MAIN SCRAPER
# ----------------------------------------------------
def main():
	conn = get_db_conn()
	store_id = 1  # Falcon store ID

	homepage_url = "https://falcononline.co.uk/"
	html = fetch_html(homepage_url)
	soup = BeautifulSoup(html, "html.parser")

	category_links = soup.select("a.level-top")
	EXCLUDE = {
		"My Account",
		"Sign In",
		"Create an Account",
		"Contact Us",
		"About Us"
	}

	seen = set()
	print(f"Found {len(category_links)} categories")

	for link in category_links:
		name = link.get_text(strip=True)
	
		url = link.get("href")

		if name in EXCLUDE:
			continue

		if name in seen:
			continue
		seen.add(name)

		# Normalize and guard top-level URLs too
		url = (url or "").strip()
		if not (url.startswith("http://") or url.startswith("https://")):
			print(f"Skipping non-HTTP top-level category URL: {url}")
			continue

		category_id = save_category(conn, store_id, name, url)

		# Mark as top-level
		cur = conn.cursor()
		cur.execute("""
			UPDATE categories
			SET parent_id = NULL, level = 0
			WHERE id = %s
		""", (category_id,))
		conn.commit()

		print(f"Saved category: {name} (ID {category_id})")

		# Scrape subcategories
		scrape_subcategories(conn, store_id, category_id, url, level=1)


if __name__ == "__main__":
	main()
