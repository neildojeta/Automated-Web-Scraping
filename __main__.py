import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import logging
from tqdm import tqdm
import random
from asyncio import Semaphore
import os

from ._2_dl_link_search import main as dl_link_download

# Create output folder if it doesn't exist
output_folder = 'Download_Links'
os.makedirs(output_folder, exist_ok=True)

# Setup logging to both file and console (terminal)
log_file = os.path.join(output_folder, '_1_web_search.log')

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler for logging to a file
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Console handler for logging to the terminal
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter and add it to both handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

exclude_domains = ['google.com', 'accounts.google.com', 'login', 'go.microsoft.com', 'support.microsoft.com']

def is_excluded_domain(href):
    for domain in exclude_domains:
        if domain in href:
            return True
    return False

async def get_download_link(page, name, version, idx, retries=2):
    name = str(name) if name is not None else ""
    version = str(version) if version is not None else ""

    if not name or not version:
        logger.warning(f"Row {idx}: Missing name or version.")
        return None

    name_encoded = name.replace(' ', '+')
    version_encoded = version.replace(' ', '+')

    search_engines = [
        f'https://www.bing.com/search?q={name_encoded}+{version_encoded}+download&rdr=1&first=1',
        f'https://search.brave.com/search?q={name_encoded}+{version_encoded}+download',
        f'https://www.google.com/search?q={name_encoded}+{version_encoded}+download'
    ]

    keywords = ['download', 'file', 'get', 'install']

    for attempt in range(retries + 1):
        for engine_url in search_engines:
            try:
                await page.goto(engine_url)
                await page.wait_for_timeout(3000)
                await page.wait_for_selector('a[href]', timeout=10000)

                links = await page.query_selector_all('a[href]')
                for link in links:
                    href = await link.get_attribute('href')
                    if href and ((href.startswith('http') or href.startswith('https')) and not is_excluded_domain(href)):
                        if (version.lower() in href.lower()):
                            logger.info(f"Row {idx}: Download link found using {engine_url}: {href}")
                            return href
                        elif (name.lower() in href.lower()):
                            logger.info(f"Row {idx}: Download link found using {engine_url}: {href}")
                            return href
                        elif any(keyword in href.lower() for keyword in keywords):
                            logger.info(f"Row {idx}: Download link found using {engine_url}: {href}")
                            return href
                        else:
                            return "None"

            except Exception as e:
                html_content = await page.content()
                with open(f'failed_row_{idx}.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logger.error(f"Row {idx}: Error fetching download link for {name} {version} using {engine_url}: {e}")

        if attempt < retries:
            logger.info(f"Row {idx}: No valid download link found. Retrying... (Attempt {attempt + 1})")
            await asyncio.sleep(random.uniform(1, 3))

    logger.info(f"Row {idx}: No valid download link found after {retries} attempts.")
    return None

async def process_links(df, concurrency_limit=10):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        semaphore = Semaphore(concurrency_limit)

        async def process_row(idx, row):
            async with semaphore:
                page = await browser.new_page()
                await asyncio.sleep(random.uniform(0.5, 1.5))
                link = await get_download_link(page, row['name'], row['version'], idx)
                await page.close()
                return link

        tasks = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
            tasks.append(asyncio.ensure_future(process_row(idx, row)))

        download_links = await asyncio.gather(*tasks)
        await browser.close()
        return download_links

def main():
    input_file = 'cleaned_data.csv'
    df = pd.read_csv(input_file)

    logger.info("Starting the download link extraction process.")

    concurrency_limit = 10
    download_links = asyncio.run(process_links(df, concurrency_limit))

    df['Download Link'] = download_links

    output_file = os.path.join(output_folder, '_1_web_search.csv')
    df.to_csv(output_file, index=False)

    logger.info(f"Results saved to {output_file}")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
    dl_link_download()
