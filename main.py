import requests
from bs4 import BeautifulSoup
import json
import argparse
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Base URL for Hacker News
BASE_URL = 'https://news.ycombinator.com/'

def parse_html(html):
    # Extracts article data from HTML content
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('tr', class_='athing')
    
    posts = []
    for article in articles:
        # Extract position, title, and link
        position = int(article.find('span', class_='rank').text.strip().rstrip('.'))
        title_span = article.find('span', class_='titleline')
        title = title_span.a.text.strip()
        link = urljoin(BASE_URL, title_span.a['href'])  # Convert to absolute URL on partial links
        
        # Extract points from the next row, matching by id
        article_id = article['id']
        score_span = article.find_next_sibling('tr').find('span', id=f'score_{article_id}')
        
        points = int(score_span.text.split()[0]) if score_span else 0  # Posts without points are mostly ads
        
        posts.append({'position': position, 'title': title, 'points': points, 'link': link})

    return posts

def fetch_page(url):
    # Fetches data from a given URL with retry logic
    for _ in range(3):  # Retry up to 3 times
        response = requests.get(url)
        if response.status_code == 200:
            return parse_html(response.text)
        print(f"Failed to retrieve data from {url}. Status code: {response.status_code}. Retrying...")
        time.sleep(1)  # Wait before retrying

    return []  # Return empty if all attempts fail

def fetch_all_pages(max_workers):
    # Fetches all pages concurrently until no more posts are found
    all_posts = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        page_number = 1
        
        while True:
            url = BASE_URL if page_number == 1 else f'{BASE_URL}?p={page_number}'
            future = executor.submit(fetch_page, url)
            futures[future] = page_number
            
            for completed_future in as_completed(futures.keys()):
                extracted_posts = completed_future.result()
                if not extracted_posts:  # Stop if a page returns no posts. It is either reaching the limit or we are getting rate limited
                    print(f"No posts found for page {futures[completed_future]}. Stopping fetch.")
                    return all_posts
                
                all_posts.extend(extracted_posts)
                del futures[completed_future]

            page_number += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Hacker News articles.")
    parser.add_argument('--all-pages', action='store_true', default=False, help='Fetch all paginated pages if set to True.')
    parser.add_argument('--max-workers', type=int, default=8, help='Number of concurrent threads (default: 8).')
    
    args = parser.parse_args()
    
    all_posts = fetch_all_pages(args.max_workers) if args.all_pages else fetch_page(BASE_URL)

    # Sort posts by position before printing
    result = sorted(all_posts, key=lambda x: x['position'])

    # Print the extracted data in pretty JSON format
    print(json.dumps(result, indent=2))
