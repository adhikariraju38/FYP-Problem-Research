"""
Apple App Store Reviews Scraper for 'The For You' Page Problem Research
========================================================================
Scrapes reviews from Apple App Store RSS feed for TikTok, Instagram, YouTube, Facebook.
NO API KEY REQUIRED - uses Apple's public RSS feed.

Usage:
    python 01d_scrape_appstore.py
    python 01d_scrape_appstore.py --max-pages 10
"""

import os
import sys
import logging
import time
import json
from datetime import datetime
from typing import List, Dict
import xml.etree.ElementTree as ET

import requests
import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = '../data/raw'

# App Store app IDs
# Find IDs at: https://apps.apple.com/app/id{APP_ID}
APP_STORE_APPS = {
    'tiktok': {
        'app_id': '835599320',
        'display_name': 'TikTok'
    },
    'instagram': {
        'app_id': '389801252',
        'display_name': 'Instagram'
    },
    'youtube': {
        'app_id': '544007664',
        'display_name': 'YouTube'
    },
    'facebook': {
        'app_id': '284882215',
        'display_name': 'Facebook'
    }
}

# Countries to scrape
COUNTRIES = ['us', 'gb', 'in', 'au', 'ca']

# Keywords to identify relevant reviews
RELEVANT_KEYWORDS = [
    # Explicit content
    'inappropriate', 'explicit', 'sexual', 'nudity', 'nude', '18+', 'adult',
    'vulgar', 'pornography', 'porn', 'naked', 'sexy', 'thirst trap', 'nsfw',

    # Age concerns
    'kid', 'child', 'children', 'teen', 'teenager', 'minor', 'young',
    'daughter', 'son', 'parent', 'my child', 'my kid', 'underage',

    # Mental health
    'addiction', 'addicted', 'addict', 'mental health', 'depression',
    'depressed', 'anxiety', 'anxious', 'brain rot', 'attention span',
    'dopamine', 'toxic', 'harmful',

    # Algorithm
    'algorithm', 'fyp', 'for you page', 'recommended', 'keeps showing',
    'feed', 'suggestion', 'recommends',

    # Safety and controls
    'dangerous', 'harmful', 'ban', 'restrict', 'filter', 'parental',
    'control', 'protect', 'safety', 'safe', 'block',
]


def fetch_reviews_rss(app_id: str, country: str = 'us', page: int = 1) -> List[Dict]:
    """
    Fetch reviews from Apple's RSS feed.

    Args:
        app_id: App Store app ID
        country: Country code
        page: Page number (1-10)

    Returns:
        List of review dictionaries
    """
    # Apple RSS feed URL for reviews (JSON format)
    url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"

    reviews = []

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Extract entries (reviews)
        feed = data.get('feed', {})
        entries = feed.get('entry', [])

        # First entry is app info, skip it
        if entries and 'im:name' in entries[0]:
            entries = entries[1:]

        for entry in entries:
            try:
                review = {
                    'review_id': entry.get('id', {}).get('label', ''),
                    'title': entry.get('title', {}).get('label', ''),
                    'review_text': entry.get('content', {}).get('label', ''),
                    'rating': int(entry.get('im:rating', {}).get('label', 0)),
                    'author': entry.get('author', {}).get('name', {}).get('label', ''),
                    'version': entry.get('im:version', {}).get('label', ''),
                    'vote_count': int(entry.get('im:voteCount', {}).get('label', 0)),
                    'vote_sum': int(entry.get('im:voteSum', {}).get('label', 0)),
                }
                reviews.append(review)
            except Exception as e:
                continue

    except requests.exceptions.RequestException as e:
        logger.debug(f"Request error for {country} page {page}: {str(e)}")
    except json.JSONDecodeError as e:
        logger.debug(f"JSON error for {country} page {page}: {str(e)}")
    except Exception as e:
        logger.debug(f"Error fetching {country} page {page}: {str(e)}")

    return reviews


def scrape_app_reviews(app_key: str, country: str = 'us', max_pages: int = 10) -> List[Dict]:
    """
    Scrape reviews from App Store for a single app.

    Args:
        app_key: Key from APP_STORE_APPS dict
        country: Country code
        max_pages: Maximum pages to fetch (10 reviews per page, max 10 pages)

    Returns:
        List of review dictionaries
    """
    app_info = APP_STORE_APPS.get(app_key)
    if not app_info:
        logger.error(f"Unknown app: {app_key}")
        return []

    all_reviews = []

    for page in range(1, min(max_pages + 1, 11)):  # Max 10 pages per Apple's limit
        reviews = fetch_reviews_rss(app_info['app_id'], country, page)

        if not reviews:
            break

        for review in reviews:
            review['app_name'] = app_key
            review['platform'] = app_key
            review['country'] = country
            review['source'] = 'app_store'
            review['scraped_at'] = datetime.now().isoformat()

        all_reviews.extend(reviews)
        time.sleep(0.5)  # Rate limiting

    return all_reviews


def is_relevant(text: str) -> bool:
    """Check if review text contains relevant keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in RELEVANT_KEYWORDS)


def categorize_keywords(text: str) -> Dict[str, bool]:
    """Categorize which keyword groups are present."""
    if not text:
        return {cat: False for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']}

    text_lower = text.lower()

    return {
        'explicit_content': any(k in text_lower for k in ['inappropriate', 'explicit', 'sexual', 'nudity', 'nude', 'porn', 'naked', 'nsfw', 'adult content']),
        'age_concerns': any(k in text_lower for k in ['kid', 'child', 'children', 'teen', 'teenager', 'minor', 'young', 'daughter', 'son', 'underage']),
        'mental_health': any(k in text_lower for k in ['addiction', 'addicted', 'mental health', 'depression', 'anxiety', 'brain rot', 'dopamine', 'toxic']),
        'algorithm': any(k in text_lower for k in ['algorithm', 'fyp', 'for you', 'recommended', 'keeps showing', 'feed', 'suggestion']),
        'parental_controls': any(k in text_lower for k in ['parental', 'control', 'filter', 'restrict', 'block', 'protect', 'safety', 'ban'])
    }


def main(max_pages: int = 10, countries: List[str] = None):
    """
    Main function to scrape App Store reviews.

    Args:
        max_pages: Max pages per app per country (10 reviews/page, max 10 pages)
        countries: List of country codes to scrape
    """
    if countries is None:
        countries = COUNTRIES

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("APP STORE REVIEWS SCRAPER (RSS Feed)")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Apps: {list(APP_STORE_APPS.keys())}")
    logger.info(f"Countries: {countries}")
    logger.info(f"Max pages per app/country: {max_pages}")
    logger.info(f"Expected max reviews: {len(APP_STORE_APPS) * len(countries) * max_pages * 10}")

    all_reviews = []

    # Scrape each app in each country
    total_tasks = len(APP_STORE_APPS) * len(countries)

    for app_key in APP_STORE_APPS.keys():
        logger.info(f"\nScraping {APP_STORE_APPS[app_key]['display_name']}...")
        for country in tqdm(countries, desc=f"  {app_key}"):
            reviews = scrape_app_reviews(app_key, country, max_pages)
            all_reviews.extend(reviews)
            time.sleep(1)  # Rate limiting between countries

    if not all_reviews:
        logger.error("No reviews scraped. Apple may be rate-limiting.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_reviews)

    # Add relevance flag and categories
    df['is_relevant'] = df['review_text'].apply(is_relevant)

    # Add keyword categories
    categories = df['review_text'].apply(categorize_keywords)
    for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
        df[cat] = categories.apply(lambda x: x.get(cat, False))

    # Filter relevant
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'appstore_reviews_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'appstore_reviews_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("APP STORE SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total reviews: {len(df)}")
    logger.info(f"Relevant reviews: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}% hit rate)")

    logger.info("\nBy App:")
    for app in df['app_name'].unique():
        count = len(df[df['app_name'] == app])
        relevant = len(df[(df['app_name'] == app) & (df['is_relevant'])])
        logger.info(f"  {app}: {count} total, {relevant} relevant")

    logger.info("\nBy Country:")
    for country in df['country'].unique():
        count = len(df[df['country'] == country])
        logger.info(f"  {country}: {count}")

    if len(relevant_df) > 0:
        logger.info("\nKeyword Categories in Relevant Reviews:")
        for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
            count = relevant_df[cat].sum()
            pct = count / len(relevant_df) * 100
            logger.info(f"  {cat}: {count} ({pct:.1f}%)")

    logger.info(f"\nSaved to:")
    logger.info(f"  All reviews: {all_path}")
    logger.info(f"  Relevant reviews: {relevant_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape App Store reviews via RSS")
    parser.add_argument('--max-pages', type=int, default=10,
                       help='Max pages per app per country (default: 10, max 10)')
    parser.add_argument('--countries', type=str, nargs='+',
                       default=['us', 'gb', 'in', 'au', 'ca'],
                       help='Countries to scrape (default: us gb in au ca)')

    args = parser.parse_args()
    main(max_pages=args.max_pages, countries=args.countries)
