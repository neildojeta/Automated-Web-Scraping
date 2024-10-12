import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import logging
from tqdm import tqdm
import random
from asyncio import Semaphore

# Setup logging
logging.basicConfig(filename='_1_web_search.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

exclude_domains = ['google.com', 'accounts.google.com', 'login', 'go.microsoft.com', 'support.microsoft.com']

# Function to check if the link is from an excluded domain
def is_excluded_domain(href):
    for domain in exclude_domains:
        if domain in href:
            return True
    return False

# Function to get download link using Playwright (headless browser)
async def get_download_link(page, name, version, idx, retries=2):
    name = str(name) if name is not None else ""
    version = str(version) if version is not None else ""

    if not name or not version:
        logging.warning(f"Row {idx}: Missing name or version.")
        return None

    # Encode the name and version to handle spaces and special characters
    name_encoded = name.replace(' ', '+')
    version_encoded = version.replace(' ', '+')

    # Define the search engines with their corresponding URLs
    search_engines = [
        f'https://www.bing.com/search?q={name_encoded}+{version_encoded}+download&rdr=1&first=1'
        # f'https://search.brave.com/search?q={name_encoded}+{version_encoded}+download',
        f'https://www.google.com/search?q={name_encoded}+{version_encoded}+download'
    ]

    keywords = ['download', 'file', 'get', 'install']

    for attempt in range(retries + 1):  # Allow up to `retries` attempts
        for engine_url in search_engines:
            try:
                await page.goto(engine_url)
                await page.wait_for_timeout(3000)  # Wait for 3 seconds for page to load fully
                await page.wait_for_selector('a[href]', timeout=10000)

                # Extract all links from the search results
                links = await page.query_selector_all('a[href]')

                for link in links:
                    href = await link.get_attribute('href')
                    if href and ((href.startswith('http') or href.startswith('https')) and not is_excluded_domain(href)):
                        if any(keyword in href.lower() for keyword in keywords) or \
                                (name.lower() in href.lower() and version.lower() in href.lower()):
                            logging.info(f"Row {idx}: Download link found using {engine_url}: {href}")
                            return href

            except Exception as e:
                html_content = await page.content()
                with open(f'failed_row_{idx}.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logging.error(f"Row {idx}: Error fetching download link for {name} {version} using {engine_url}: {e}")

        # If no link was found in this attempt
        if attempt < retries:
            logging.info(f"Row {idx}: No valid download link found. Retrying... (Attempt {attempt + 1})")
            await asyncio.sleep(random.uniform(1, 3))  # Optional delay before retrying

    logging.info(f"Row {idx}: No valid download link found after {retries} attempts.")
    return None

# Asynchronous main function to process the DataFrame rows using concurrency
async def process_links(df, concurrency_limit=10):
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        semaphore = Semaphore(concurrency_limit)

        async def process_row(idx, row):
            async with semaphore:
                # Create a new tab (page) for each task
                page = await browser.new_page()

                # Random delay to avoid detection
                await asyncio.sleep(random.uniform(0.5, 1.5))

                # Fetch download link
                link = await get_download_link(page, row['name'], row['version'], idx)
                await page.close()

                return link

        tasks = []
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
            tasks.append(asyncio.ensure_future(process_row(idx, row)))

        # Gather all tasks to run concurrently with the defined concurrency limit
        download_links = await asyncio.gather(*tasks)

        await browser.close()
        return download_links


def main():
    # Read the data from the CSV file
    input_file = 'cleaned_data.csv'
    df = pd.read_csv(input_file)

    logging.info("Starting the download link extraction process.")

    # Set concurrency limit (e.g., 10 simultaneous browser pages)
    concurrency_limit = 10

    # Run the asynchronous event loop with concurrency
    download_links = asyncio.run(process_links(df, concurrency_limit))

    # Add the download links to the DataFrame
    df['Download Link'] = download_links

    # Save the updated DataFrame to a new CSV file
    output_file = '_1_web_search.csv'
    df.to_csv(output_file, index=False)

    logging.info(f"Results saved to {output_file}")
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
