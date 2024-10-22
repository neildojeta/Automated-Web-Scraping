import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import logging
from tqdm import tqdm
from urllib.parse import urljoin
import os

# Create output folder if it doesn't exist
output_folder = 'Download_Links'
os.makedirs(output_folder, exist_ok=True)

# Setup logging to both file and console (terminal)
log_file = os.path.join(output_folder, '_2_dl_link_search.log')

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

def complete_url(base_url, link):
    if link.startswith('//'):
        return 'https:' + link
    elif link.startswith('/'):
        return urljoin(base_url, link)
    elif link.startswith(''):
        return urljoin(base_url, link)
    else:
        return link

async def extract_file_links(page, url, idx, depth=0, max_depth=3):
    retries = 3
    tries = 3
    timeout = 50000  # 30 seconds
    download_link = None

    if url == "None":
        return "None"

    while retries > 0:
        try:
            await page.goto(url, timeout=timeout)
            await page.wait_for_load_state('networkidle', timeout=timeout)

            base_url = page.url

            # Monitor network requests for a potential download link
            def handle_request(request):
                nonlocal download_link
                if request.resource_type in ["xhr", "document"]:
                    if request.url.endswith(('.exe', '.zip', '.dmg', '.msix', '.xz')):
                        download_link = request.url
                        logger.info(f"Row {idx}: (FOUND1)Download link captured from network request: {download_link}")

            page.on('request', handle_request)

            # Priority 1: Check for direct file download with these extensions
            download_button = await page.query_selector('a[href$=".exe"], a[href$=".zip"], a[href$=".dmg"], a[href$=".msix"], a[href$=".xz"]')
            if download_button:
                href = await download_button.get_attribute('href')
                if href:
                    complete_link = complete_url(base_url, href)
                    logger.info(f"Row {idx}: (FOUND2)Direct download link found: {complete_link}")
                    return complete_link
                    
            else:
                logger.info(f"Row {idx}: No direct download link found, searching for download buttons.")

            # Priority 2: Check for download button elements
            try:
                download_button = await page.query_selector('[class*="download"]')
                if download_button and depth < max_depth:
                    logger.info(f"Row {idx}: Simulating click for download button on {url}")
                    await download_button.click()
                    await page.wait_for_timeout(5000)  # Wait for network events to fire
                    if download_link:
                        return download_link
                    return await extract_file_links(page, page.url, idx, depth + 1, max_depth)
            except Exception as e:
                logger.warning(f"Row {idx}: Error simulating click or finding download link on {url}: {e}")

            # Priority 3: Check for hrefs in <a> tags with 'download' in their attributes or class
            try:
                links = await page.query_selector_all('a, [href*="/download/download"], [href*="download"], [class*="download"], [title*="download"]')
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        complete_link = complete_url(base_url, href)

                        if complete_link.endswith(('.exe', '.zip', '.dmg', '.msix', '.xz')):
                            logger.info(f"Row {idx}: (FOUND3)Direct download link found on {url}")
                            return complete_link

                        elif complete_link.lower().endswith("download"):
                            logger.info(f"Row {idx}: Found download link in href or title on {url}")
                            return complete_link

            except Exception as e:
                logger.warning(f"Row {idx}: Error finding hrefs or checking download links: {e}")

            logger.info(f"Row {idx}: No valid download link found on {url}")
            return "None"

        except Exception as e:
            retries -= 1
            if retries == 0:
                logger.error(f"Row {idx}: Error extracting links from {url}: {e}")
                return "None"
            else:
                logger.warning(f"Row {idx}: Retry {tries - (retries)} for {url}")
                await asyncio.sleep(2)

async def process_download_links(df, output_file, concurrency_limit=10):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def process_row(idx, row):
            async with semaphore:
                page = await browser.new_page()
                url = row['Download Link']
                
                if pd.notna(row.get('Extracted Link')) and row['Extracted Link'] != "None":
                    logger.info(f"Row {idx}: Skipping, already has an extracted link.")
                    return row['Extracted Link']
                
                if pd.notna(url):
                    if url == "None":
                        df.at[idx, 'Extracted Link'] = "None"
                        df.iloc[[idx]].to_csv(output_file, mode='a', header=False, index=False)
                        logger.warning(f"Row {idx}: URL is 'None', set Extracted Link to 'None'.")
                        return "None"

                    result_link = await extract_file_links(page, url, idx)
                    await page.close()
                    
                    df.at[idx, 'Extracted Link'] = result_link
                    df.iloc[[idx]].to_csv(output_file, mode='a', header=False, index=False)
                    
                    return result_link
                else:
                    logger.warning(f"Row {idx}: No Download Link found.")
                    return None

        tasks = [process_row(idx, row) for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing download links")]
        await asyncio.gather(*tasks)

        await browser.close()

def main():
    input_file = os.path.join(output_folder, '_1_web_search.csv')
    output_file = os.path.join(output_folder, '_2_dl_link_search.csv')
    # input_file = '_1_web_search.csv'
    # output_file = '_2_dl_link_search.csv'
    df = pd.read_csv(input_file)

    if 'Extracted Link' not in df.columns:
        df['Extracted Link'] = None

    df.head(0).to_csv(output_file, index=False)

    logger.info("Starting the download button and .exe/.zip link extraction process.")

    concurrency_limit = 10
    asyncio.run(process_download_links(df, output_file, concurrency_limit))

    logger.info(f"Results saved to {output_file}")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
