import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import logging
from tqdm import tqdm
from urllib.parse import urljoin

# Setup logging
logging.basicConfig(filename='_2_dl_lonk_search.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def complete_url(base_url, link):
    if link.startswith('//'):
        return 'https:' + link
    elif link.startswith('/'):
        return urljoin(base_url, link)
    else:
        return link


async def extract_file_links(page, url, idx, depth=0, max_depth=2):
    retries = 2
    timeout = 30000  # 30 seconds
    download_link = None

    while retries > 0:
        try:
            await page.goto(url, timeout=timeout)
            await page.wait_for_load_state('networkidle', timeout=timeout)

            base_url = page.url

            # Monitor network requests for a potential download link
            def handle_request(request):
                nonlocal download_link
                #if request.resource_type == "xhr" or request.resource_type == "document":
                if request.resource_type in ["xhr", "document"]:
                    if request.url.endswith(('.exe', '.zip', '.dmg', '.msix')):
                        download_link = request.url
                        logging.info(f"Row {idx}: Download link captured from network request: {download_link}")

            page.on('request', handle_request)

            # Priority 1: Check for direct file download with these extensions
            download_button = await page.query_selector('a[href$=".exe"], a[href$=".zip"], a[href$=".dmg"], a[href$=".msix"]')
            if download_button:
                href = await download_button.get_attribute('href')
                if href:
                    complete_link = complete_url(base_url, href)
                    logging.info(f"Row {idx}: Direct download link found: {complete_link}")
                    return complete_link
            else:
                logging.info(f"Row {idx}: No direct download link found, searching for download buttons.")

            # Priority 2: Check for download button elements
            try:
                download_button = await page.query_selector('[class*="download"]')
                if download_button and depth < max_depth:
                    logging.info(f"Row {idx}: Simulating click for download button on {url}")
                    await download_button.click()
                    await page.wait_for_timeout(5000)  # Wait for network events to fire
                    if download_link:
                        return download_link
                    # Recursively check after click
                    return await extract_file_links(page, page.url, idx, depth + 1, max_depth)
            except Exception as e:
                logging.warning(f"Row {idx}: Error simulating click or finding download link on {url}: {e}")

            # Priority 3: Check for hrefs in <a> tags with 'download' in their attributes or class
            try:
                links = await page.query_selector_all('a, [href*="/download/download"], [href*="download"], [class*="download"], [title*="download"]')
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        complete_link = complete_url(base_url, href)

                        # Direct download link condition
                        if complete_link.endswith(('.exe', '.zip', '.dmg', '.msix')):
                            logging.info(f"Row {idx}: Direct download link found on {url}")
                            return complete_link

                        # Check for intermediary pages with 'download' in URL
                        elif 'download' in complete_link.lower():
                            logging.info(f"Row {idx}: Found download link in href or title on {url}")
                            return complete_link

            except Exception as e:
                logging.warning(f"Row {idx}: Error finding hrefs or checking download links: {e}")

            logging.info(f"Row {idx}: No valid download link found on {url}")
            return "None"

        except Exception as e:
            retries -= 1
            if retries == 0:
                logging.error(f"Row {idx}: Error extracting links from {url}: {e}")
                return None
            else:
                logging.warning(f"Row {idx}: Retry {4 - retries} for {url}")
                await asyncio.sleep(2)  # Short delay before retrying

async def process_download_links(df, concurrency_limit=10):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        semaphore = asyncio.Semaphore(concurrency_limit)

        async def process_row(idx, row):
            async with semaphore:
                page = await browser.new_page()
                url = row['Download Link']
                if pd.notna(url):
                    result_link = await extract_file_links(page, url, idx)
                    await page.close()
                    return result_link
                else:
                    logging.warning(f"Row {idx}: No Download Link found.")
                    return None

        tasks = [process_row(idx, row) for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing download links")]
        download_links = await asyncio.gather(*tasks)

        await browser.close()
        return download_links

def main():
    input_file = '_1_web_search.csv'
    df = pd.read_csv(input_file)

    logging.info("Starting the download button and .exe/.zip link extraction process.")

    concurrency_limit = 10
    download_links = asyncio.run(process_download_links(df, concurrency_limit))

    df['Extracted Link'] = download_links
    output_file = '_2_dl_link_search.csv'
    df.to_csv(output_file, index=False)

    logging.info(f"Results saved to {output_file}")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()