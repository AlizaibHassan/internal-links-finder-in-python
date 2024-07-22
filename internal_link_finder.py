import requests
from bs4 import BeautifulSoup
from lxml import etree
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin

# Read input CSV file
input_file = 'input.csv'
df = pd.read_csv(input_file)

# Extract data from the DataFrame
urls = df['URLs'].tolist()
target_url = df['Target URL'].iloc[0]
anchor_texts = df['Target Anchors'].dropna().tolist()
xpath = df['Xpath'].iloc[0]

results = []
total_urls = len(urls)
completed_urls = 0

# Function to get the content area using XPath and all <a> links
def get_content_area(url, xpath):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        dom = etree.HTML(str(soup))
        content_area = dom.xpath(xpath)
        if content_area:
            content_text = ''.join(content_area[0].itertext())
            links = [a.get('href') for a in content_area[0].xpath('.//a')]
            return url, content_text, links
        else:
            return url, '', []
    except Exception as e:
        print(f"Error fetching content area for {url}: {e}")
        return url, '', []

# Function to process each URL
def process_url(url):
    global completed_urls
    url, content, links = get_content_area(url, xpath)
    if not content:
        completed_urls += 1
        print_progress(completed_urls, total_urls)
        return []

    # Check if target URL is in the list of <a> links
    parsed_target_url = urlparse(target_url)
    target_paths = [target_url, parsed_target_url.path]
    
    for link in links:
        if link in target_paths or urljoin(url, link) in target_paths:
            completed_urls += 1
            print_progress(completed_urls, total_urls)
            return []

    print(f"Target URL not found in {url}")

    # Find anchor text keywords
    local_results = []
    found_anchors = []
    for anchor in anchor_texts:
        if anchor in content:
            found_anchors.append(anchor)
    
    if found_anchors:
        local_results.append({
            'URL': url,
            'Anchor Texts': ', '.join(found_anchors)
        })
    
    completed_urls += 1
    print_progress(completed_urls, total_urls)
    return local_results

# Function to print progress
def print_progress(completed, total):
    percentage = (completed / total) * 100
    print(f"Completed: {completed}/{total} ({percentage:.2f}%)")

# Use ThreadPoolExecutor for multithreading
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_url, url): url for url in urls}
    
    for future in as_completed(futures):
        url_results = future.result()
        if url_results:
            results.extend(url_results)

# Save results to CSV
df_results = pd.DataFrame(results)
df_results.to_csv('internal_link_suggestions.csv', index=False)

print("Internal link suggestions have been saved to internal_link_suggestions.csv")
