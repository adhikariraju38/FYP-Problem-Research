"""
Trustpilot Reviews Scraper for 'The For You' Page Problem Research
==================================================================
Scrapes reviews from Trustpilot for TikTok, Instagram, etc.
NO API KEY REQUIRED - uses public web pages.

Usage:
    python 01f_scrape_trustpilot.py
    python 01f_scrape_trustpilot.py --max-pages 20
"""

import os
import sys
import logging
import time
import re
import json
from datetime import datetime
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = '../data/raw'

# Trustpilot company URLs
TRUSTPILOT_COMPANIES = {
    'tiktok': 'www.tiktok.com',
    'instagram': 'www.instagram.com',
    'facebook': 'www.facebook.com',
    'youtube': 'www.youtube.com',
}

# Keywords to identify relevant reviews
RELEVANT_KEYWORDS = [
    'inappropriate', 'explicit', 'sexual', 'nudity', 'nude', '18+', 'adult',
    'vulgar', 'pornography', 'porn', 'naked', 'nsfw',
    'kid', 'child', 'children', 'teen', 'teenager', 'minor', 'young',
    'daughter', 'son', 'parent', 'underage',
    'addiction', 'addicted', 'mental health', 'depression', 'anxiety',
    'brain rot', 'attention span', 'dopamine', 'toxic', 'harmful',
    'algorithm', 'fyp', 'for you page', 'recommended', 'keeps showing',
    'dangerous', 'ban', 'restrict', 'filter', 'parental', 'control',
    'protect', 'safety', 'block',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def scrape_trustpilot_page(company_domain: str, page: int = 1) -> List[Dict]:
    """
    Scrape a single page of Trustpilot reviews.

    Args:
        company_domain: Company domain (e.g., 'www.tiktok.com')
        page: Page number

    Returns:
        List of review dictionaries
    """
    reviews = []
    url = f"https://www.trustpilot.com/review/{company_domain}?page={page}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)

        if response.status_code == 403:
            logger.warning(f"Access forbidden for {company_domain}")
            return []

        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find review cards
        review_cards = soup.find_all('article', {'data-service-review-card-paper': 'true'})

        if not review_cards:
            # Try alternative selector
            review_cards = soup.find_all('div', class_=re.compile(r'styles_cardWrapper'))

        for card in review_cards:
            try:
                # Extract rating
                rating_elem = card.find('div', {'data-service-review-rating': True})
                rating = 0
                if rating_elem:
                    rating_attr = rating_elem.get('data-service-review-rating', '0')
                    rating = int(rating_attr) if rating_attr.isdigit() else 0

                # Extract title
                title_elem = card.find('h2') or card.find('a', {'data-review-title-typography': 'true'})
                title = title_elem.get_text(strip=True) if title_elem else ''

                # Extract review text
                text_elem = card.find('p', {'data-service-review-text-typography': 'true'})
                if not text_elem:
                    text_elem = card.find('p', class_=re.compile(r'typography_body'))
                text = text_elem.get_text(strip=True) if text_elem else ''

                # Extract author
                author_elem = card.find('span', {'data-consumer-name-typography': 'true'})
                author = author_elem.get_text(strip=True) if author_elem else ''

                # Extract date
                date_elem = card.find('time')
                date = date_elem.get('datetime', '') if date_elem else ''

                if title or text:
                    reviews.append({
                        'title': title,
                        'review_text': f"{title} {text}".strip(),
                        'rating': rating,
                        'author': author,
                        'date': date,
                        'company': company_domain,
                    })

            except Exception as e:
                continue

    except requests.exceptions.RequestException as e:
        logger.debug(f"Request error for {company_domain} page {page}: {str(e)}")
    except Exception as e:
        logger.debug(f"Error parsing {company_domain} page {page}: {str(e)}")

    return reviews


def scrape_company_reviews(company_key: str, max_pages: int = 10) -> List[Dict]:
    """
    Scrape all reviews for a company from Trustpilot.

    Args:
        company_key: Key from TRUSTPILOT_COMPANIES
        max_pages: Maximum pages to scrape

    Returns:
        List of review dictionaries
    """
    company_domain = TRUSTPILOT_COMPANIES.get(company_key)
    if not company_domain:
        logger.error(f"Unknown company: {company_key}")
        return []

    all_reviews = []

    for page in range(1, max_pages + 1):
        reviews = scrape_trustpilot_page(company_domain, page)

        if not reviews:
            break

        for review in reviews:
            review['app_name'] = company_key
            review['platform'] = company_key
            review['source'] = 'trustpilot'
            review['scraped_at'] = datetime.now().isoformat()

        all_reviews.extend(reviews)
        time.sleep(2)  # Rate limiting

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
        'explicit_content': any(k in text_lower for k in ['inappropriate', 'explicit', 'sexual', 'nudity', 'nude', 'porn', 'naked', 'nsfw']),
        'age_concerns': any(k in text_lower for k in ['kid', 'child', 'children', 'teen', 'teenager', 'minor', 'young', 'daughter', 'son', 'underage']),
        'mental_health': any(k in text_lower for k in ['addiction', 'addicted', 'mental health', 'depression', 'anxiety', 'brain rot', 'dopamine', 'toxic']),
        'algorithm': any(k in text_lower for k in ['algorithm', 'fyp', 'for you', 'recommended', 'keeps showing', 'feed', 'suggestion']),
        'parental_controls': any(k in text_lower for k in ['parental', 'control', 'filter', 'restrict', 'block', 'protect', 'safety', 'ban'])
    }


def main(max_pages: int = 20):
    """
    Main function to scrape Trustpilot reviews.

    Args:
        max_pages: Max pages per company
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("TRUSTPILOT REVIEWS SCRAPER")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Companies: {list(TRUSTPILOT_COMPANIES.keys())}")
    logger.info(f"Max pages per company: {max_pages}")

    all_reviews = []

    for company_key in tqdm(TRUSTPILOT_COMPANIES.keys(), desc="Scraping"):
        logger.info(f"\nScraping {company_key}...")
        reviews = scrape_company_reviews(company_key, max_pages)
        all_reviews.extend(reviews)
        logger.info(f"  Got {len(reviews)} reviews")
        time.sleep(3)  # Rate limiting between companies

    if not all_reviews:
        logger.error("No reviews scraped. Trustpilot may be blocking requests.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_reviews)

    # Add relevance flag
    df['is_relevant'] = df['review_text'].apply(is_relevant)

    # Add keyword categories
    categories = df['review_text'].apply(categorize_keywords)
    for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
        df[cat] = categories.apply(lambda x: x.get(cat, False))

    # Filter relevant
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'trustpilot_reviews_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'trustpilot_reviews_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TRUSTPILOT SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total reviews: {len(df)}")
    logger.info(f"Relevant reviews: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}% hit rate)")

    logger.info("\nBy Company:")
    for company in df['app_name'].unique():
        count = len(df[df['app_name'] == company])
        relevant = len(df[(df['app_name'] == company) & (df['is_relevant'])])
        logger.info(f"  {company}: {count} total, {relevant} relevant")

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

    parser = argparse.ArgumentParser(description="Scrape Trustpilot reviews")
    parser.add_argument('--max-pages', type=int, default=20,
                       help='Max pages per company (default: 20)')

    args = parser.parse_args()
    main(max_pages=args.max_pages)
