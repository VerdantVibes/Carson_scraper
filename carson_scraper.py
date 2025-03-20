import requests
from bs4 import BeautifulSoup
import csv
import json
import logging
import os
from datetime import datetime
import time
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_existing_products():
    existing_products = set()
    if os.path.exists('carson_products.csv'):
        with open('carson_products.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Store SKU as the unique identifier
                existing_products.add(row['sku'])
    return existing_products

def save_html_response(response_text, page_number):
    # Create responses directory if it doesn't exist
    os.makedirs('responses', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'responses/carson_page_{page_number}_{timestamp}.html'
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(response_text)
    logger.info(f"Saved raw HTML response to {filename}")
    return filename

def parse_products(json_response):
    try:
        data = json.loads(json_response)
        html_content = data.get('products_html', '')
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    products = []
    
    for product_div in soup.find_all('div', class_='product'):
        product = {}
        
        # Add sequential number
        product['no'] = len(products) + 1
        
        # Extract category
        subtitle = product_div.find('p', class_='product__subtitle')
        product['category'] = subtitle.text.strip() if subtitle else ''
        
        # Extract title
        title = product_div.find('h3', class_='product__title')
        product['title'] = title.text.strip() if title else ''
        
        # Extract SKU
        sku = product_div.find('small', class_='product__sku')
        product['sku'] = sku.text.strip() if sku else ''
        
        # Extract prices
        price_div = product_div.find('div', class_='product__price')
        price_p = product_div.find('p', class_='product__price')
        
        if price_div:  # Case for discounted price
            # Check if it's a discounted price (has both prices)
            prices = price_div.find_all('p')
            if prices:  # Has original price in <p> tag
                try:
                    # First text is discount price
                    discount_text = price_div.contents[0].strip()
                    discount_price = float(discount_text.replace('€', '').replace(',', '').strip())
                    # Price in <p> tag is original price
                    original_text = prices[0].text.strip()
                    original_price = float(original_text.replace('€', '').replace(',', '').strip())
                    product['discount_price'] = discount_price
                    product['original_price'] = original_price
                except (ValueError, IndexError):
                    product['discount_price'] = ''
                    product['original_price'] = ''
        elif price_p:  # Case for single price
            try:
                # Get direct text content without span
                price_text = price_p.find(text=True, recursive=False).strip()
                price = float(price_text.replace('€', '').replace(',', '').strip())
                product['original_price'] = price
                product['discount_price'] = ''
            except (ValueError, IndexError, AttributeError):
                product['original_price'] = ''
                product['discount_price'] = ''
        else:  # No price found
            product['original_price'] = ''
            product['discount_price'] = ''
        
        # Extract description
        description = product_div.find('p', class_='product__text')
        product['description'] = description.text.strip() if description else ''
        
        # Extract image URL
        img = product_div.find('img')
        product['image_url'] = img.get('src') if img else ''
        
        # Extract product URL
        link = product_div.find('a', class_='product_main__link')
        if link and link.get('href'):
            product['url'] = link.get('href')
            product['additional_images'] = get_product_details(link.get('href'))
        else:
            product['url'] = ''
            product['additional_images'] = ''
        
        products.append(product)
    
    return products

def get_product_details(url):
    try:
        full_url = f"https://www.carson-modelsport.com{url}"
        response = requests.get(full_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get all image URLs from the product detail stack
        image_urls = []
        image_stack = soup.select("div.product_detail__stack img")
        if image_stack:
            for img in image_stack:
                src = img.get('src')
                if src:
                    image_urls.append(src)
        
        # Convert list to JSON string
        return json.dumps(image_urls) if image_urls else '[]'
        
    except Exception as e:
        logger.error(f"Error getting product details: {e}")
        return '[]'

def scrape_carson():
    url = 'https://www.carson-modelsport.com/carson_en/brands/carson/'
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.carson-modelsport.com/carson_en/brands/carson/',
        'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    # Load existing products
    existing_products = load_existing_products()
    logger.info(f"Found {len(existing_products)} existing products")

    page = 1
    total_new_products = 0
    existing_products = set()  # Store SKU+title combinations
    
    # Create CSV file with headers if it doesn't exist
    csv_fields = ['no', 'title', 'sku', 'category', 'original_price', 'discount_price', 'description', 'image_url', 'url', 'additional_images']
    if not os.path.exists('carson_products.csv'):
        with open('carson_products.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields)
            writer.writeheader()
    else:
        # Load existing products into set
        with open('carson_products.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_products.add((row['sku'], row['title']))

    try:
        while True:
            params = {
                'lp': str(page),
                'ajax': 'true'
            }
            
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            # Save raw HTML response
            html_file = save_html_response(response.text, page)
            
            # Parse the products for current page
            products = parse_products(response.text)
            
            if not products:
                break
            
            # Filter out existing products by SKU
            new_products = []
            for product in products:
                if product['sku'] not in existing_products:
                    new_products.append(product)
                    existing_products.add(product['sku'])
            
            if not new_products:
                logger.info("No new products found. Stopping scraper.")
                break
                
            total_new_products += len(new_products)
            
            # Save to CSV immediately after each page
            with open('carson_products.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
                # Add sequential numbers to new products
                base_number = total_new_products - len(new_products) + 1
                for i, product in enumerate(new_products):
                    product['no'] = base_number + i
                # Filter products to only include fields in csv_fields
                filtered_products = [filter_product_fields(p, csv_fields) for p in new_products]
                writer.writerows(filtered_products)
            
            logger.info(f"Scraped page {page}, found {len(products)} products, {len(new_products)} new products")
            page += 1
            
            # Add 1 second delay between requests
            time.sleep(0.5)
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    logger.info(f"Scraping completed. Total new products added: {total_new_products}")

    convert_to_excel()

def clean_text_for_excel(text):
    if not isinstance(text, str):
        return text
    # Remove or replace problematic characters
    illegal_chars = ['\r', '\n', '\t', '\u2018', '\u2019', '\u201c', '\u201d', '´', '°']
    text = text.strip()
    for char in illegal_chars:
        text = text.replace(char, ' ')
    # Replace multiple spaces with single space
    text = ' '.join(text.split())
    # Truncate if too long
    return text[:32000] if len(text) > 32000 else text

def convert_to_excel():
    if os.path.exists('carson_products.csv'):
        df = pd.read_csv('carson_products.csv')
        # Clean description field
        df['description'] = df['description'].apply(clean_text_for_excel)
        excel_file = 'carson_products.xlsx'
        df.to_excel(excel_file, index=False)
        logger.info(f"Converted CSV to Excel: {excel_file}")

def filter_product_fields(product, allowed_fields):
    return {k: v for k, v in product.items() if k in allowed_fields}

if __name__ == "__main__":
    scrape_carson()
