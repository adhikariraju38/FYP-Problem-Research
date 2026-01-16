"""
Twitter/X Scraper for 'The For You' Page Problem Research
=========================================================
Scrapes tweets using Nitter (privacy-friendly Twitter frontend).
NO API KEY REQUIRED - uses public Nitter instances.

Usage:
    python 01g_scrape_twitter.py
    python 01g_scrape_twitter.py --max-tweets 100
"""

import os
import sys
import logging
import time
import re
from datetime import datetime
from typing import List, Dict
from urllib.parse import quote

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

# Nitter instances (public, no auth needed)
# These rotate if one fails
NITTER_INSTANCES = [
    'https://nitter.poast.org',
    'https://nitter.privacydev.net',
    'https://nitter.woodland.cafe',
    'https://nitter.esmailelbob.xyz',
    'https://n.opnxng.com',
    'https://nitter.d420.de',
]

# Search queries for Twitter
SEARCH_QUERIES = [
    # TikTok concerns
    'tiktok inappropriate content',
    'tiktok kids safety',
    'tiktok explicit content',
    'tiktok addiction children',
    'tiktok algorithm problem',
    'tiktok fyp inappropriate',
    'tiktok ban kids',
    'tiktok dangerous children',

    # Instagram Reels
    'instagram reels inappropriate',
    'reels explicit content',
    'reels kids safety',

    # YouTube Shorts
    'youtube shorts inappropriate',
    'shorts kids safety',

    # General
    'social media addiction teenagers',
    'social media mental health kids',
    'algorithm showing inappropriate',
    'parental controls tiktok',

    # Nepal specific
    'tiktok nepal ban',
    'tiktok nepal',
]

# Keywords to identify relevant tweets
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


def get_working_nitter_instance() -> str:
    """Find a working Nitter instance."""
    for instance in NITTER_INSTANCES:
        try:
            response = requests.get(f"{instance}/search", headers=HEADERS, timeout=10)
            if response.status_code == 200:
                logger.info(f"Using Nitter instance: {instance}")
                return instance
        except:
            continue

    logger.warning("No Nitter instances available, trying direct method")
    return None


def search_nitter(query: str, instance: str, max_tweets: int = 50) -> List[Dict]:
    """
    Search for tweets using Nitter.

    Args:
        query: Search query
        instance: Nitter instance URL
        max_tweets: Maximum tweets to fetch

    Returns:
        List of tweet dictionaries
    """
    tweets = []
    encoded_query = quote(query)
    url = f"{instance}/search?f=tweets&q={encoded_query}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)

        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find tweet containers
        tweet_containers = soup.find_all('div', class_='timeline-item')

        for container in tweet_containers[:max_tweets]:
            try:
                # Extract tweet content
                content_elem = container.find('div', class_='tweet-content')
                if not content_elem:
                    continue

                tweet_text = content_elem.get_text(strip=True)

                # Extract username
                username_elem = container.find('a', class_='username')
                username = username_elem.get_text(strip=True) if username_elem else ''

                # Extract display name
                fullname_elem = container.find('a', class_='fullname')
                fullname = fullname_elem.get_text(strip=True) if fullname_elem else ''

                # Extract date
                date_elem = container.find('span', class_='tweet-date')
                date_link = date_elem.find('a') if date_elem else None
                date = date_link.get('title', '') if date_link else ''

                # Extract stats
                stats = container.find_all('span', class_='tweet-stat')
                replies = 0
                retweets = 0
                likes = 0

                for stat in stats:
                    stat_text = stat.get_text(strip=True)
                    icon = stat.find('span', class_='icon-container')
                    if icon:
                        icon_class = icon.get('class', [])
                        if 'icon-comment' in str(icon_class):
                            replies = int(re.sub(r'[^\d]', '', stat_text) or 0)
                        elif 'icon-retweet' in str(icon_class):
                            retweets = int(re.sub(r'[^\d]', '', stat_text) or 0)
                        elif 'icon-heart' in str(icon_class):
                            likes = int(re.sub(r'[^\d]', '', stat_text) or 0)

                # Extract tweet link
                link_elem = container.find('a', class_='tweet-link')
                tweet_link = f"{instance}{link_elem.get('href', '')}" if link_elem else ''

                tweets.append({
                    'tweet_id': tweet_link.split('/')[-1] if tweet_link else '',
                    'text': tweet_text,
                    'username': username,
                    'fullname': fullname,
                    'date': date,
                    'replies': replies,
                    'retweets': retweets,
                    'likes': likes,
                    'url': tweet_link,
                    'search_query': query,
                })

            except Exception as e:
                continue

    except Exception as e:
        logger.debug(f"Error searching Nitter for '{query}': {str(e)}")

    return tweets


def search_twitter_guest(query: str, max_tweets: int = 50) -> List[Dict]:
    """
    Alternative: Search Twitter using guest token (may not work).

    Args:
        query: Search query
        max_tweets: Maximum tweets

    Returns:
        List of tweet dictionaries
    """
    # This is a fallback that may have limited success
    tweets = []

    try:
        # Twitter's search suggestions API (public)
        url = f"https://twitter.com/i/api/1.1/search/typeahead.json"
        params = {
            'q': query,
            'src': 'search_box',
            'result_type': 'events',
        }

        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        # This endpoint is very limited, mostly for autocomplete

    except Exception as e:
        pass

    return tweets


def is_relevant(text: str) -> bool:
    """Check if tweet text contains relevant keywords."""
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


def main(max_tweets_per_query: int = 50):
    """
    Main function to scrape Twitter/X.

    Args:
        max_tweets_per_query: Max tweets per search query
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("TWITTER/X SCRAPER (via Nitter)")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Search queries: {len(SEARCH_QUERIES)}")
    logger.info(f"Max tweets per query: {max_tweets_per_query}")

    # Find working Nitter instance
    nitter_instance = get_working_nitter_instance()

    if not nitter_instance:
        logger.error("No Nitter instances available. Twitter scraping requires working Nitter.")
        logger.info("Alternative: Use Twitter API with developer account")
        return

    all_tweets = []
    seen_ids = set()

    for query in tqdm(SEARCH_QUERIES, desc="Searching Twitter"):
        tweets = search_nitter(query, nitter_instance, max_tweets_per_query)

        new_count = 0
        for tweet in tweets:
            tweet_id = tweet.get('tweet_id', '')
            if tweet_id and tweet_id not in seen_ids:
                seen_ids.add(tweet_id)
                tweet['source'] = 'twitter'
                tweet['platform'] = 'twitter'
                tweet['review_text'] = tweet['text']  # For compatibility
                tweet['scraped_at'] = datetime.now().isoformat()
                all_tweets.append(tweet)
                new_count += 1

        logger.info(f"  '{query}': {new_count} new tweets")
        time.sleep(3)  # Rate limiting

    if not all_tweets:
        logger.error("No tweets scraped. Nitter may be blocking or rate limiting.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_tweets)

    # Add relevance flag
    df['is_relevant'] = df['text'].apply(is_relevant)

    # Add keyword categories
    categories = df['text'].apply(categorize_keywords)
    for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
        df[cat] = categories.apply(lambda x: x.get(cat, False))

    # Filter relevant
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'twitter_tweets_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'twitter_tweets_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TWITTER SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total tweets: {len(df)}")
    logger.info(f"Relevant tweets: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}%)")

    if len(df) > 0:
        logger.info(f"\nEngagement Stats:")
        logger.info(f"  Total likes: {df['likes'].sum()}")
        logger.info(f"  Total retweets: {df['retweets'].sum()}")
        logger.info(f"  Avg likes per tweet: {df['likes'].mean():.1f}")

    if len(relevant_df) > 0:
        logger.info("\nKeyword Categories in Relevant Tweets:")
        for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
            count = relevant_df[cat].sum()
            pct = count / len(relevant_df) * 100
            logger.info(f"  {cat}: {count} ({pct:.1f}%)")

        # Show sample tweets
        logger.info("\nSample Relevant Tweets:")
        for _, row in relevant_df.head(3).iterrows():
            logger.info(f"\n  @{row['username']}: {row['text'][:80]}...")
            logger.info(f"    Likes: {row['likes']}, Retweets: {row['retweets']}")

    logger.info(f"\nSaved to:")
    logger.info(f"  All tweets: {all_path}")
    logger.info(f"  Relevant tweets: {relevant_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Twitter/X via Nitter")
    parser.add_argument('--max-tweets', type=int, default=200,
                       help='Max tweets per search query (default: 200)')

    args = parser.parse_args()
    main(max_tweets_per_query=args.max_tweets)
