"""
Reddit Scraper for 'The For You' Page Problem Research
=======================================================
Scrapes posts and comments from relevant subreddits.
NO API KEY REQUIRED - uses public JSON endpoints.

Usage:
    python 01e_scrape_reddit.py
    python 01e_scrape_reddit.py --max-posts 100
"""

import os
import sys
import logging
import time
import json
from datetime import datetime
from typing import List, Dict

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

# Subreddits to scrape
SUBREDDITS = [
    'tiktok',
    'Instagram',
    'youtube',
    'socialmedia',
    'Parenting',
    'internetparents',
    'teenagers',
    'Nepal',
    'india',
    'privacy',
]

# Search queries for Reddit
SEARCH_QUERIES = [
    'tiktok inappropriate content',
    'tiktok kids safety',
    'tiktok explicit',
    'tiktok addiction',
    'tiktok algorithm problem',
    'reels inappropriate',
    'social media children danger',
    'tiktok ban',
    'fyp problem',
    'tiktok mental health',
    'shorts inappropriate content',
]

# Keywords to identify relevant posts
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

# Headers to mimic browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def fetch_subreddit_posts(subreddit: str, sort: str = 'hot', limit: int = 100) -> List[Dict]:
    """
    Fetch posts from a subreddit using public JSON API.

    Args:
        subreddit: Subreddit name
        sort: Sort method (hot, new, top, rising)
        limit: Max posts to fetch (max 100 per request)

    Returns:
        List of post dictionaries
    """
    posts = []
    after = None
    fetched = 0

    while fetched < limit:
        try:
            url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
            params = {'limit': min(100, limit - fetched)}
            if after:
                params['after'] = after

            response = requests.get(url, headers=HEADERS, params=params, timeout=30)

            if response.status_code == 429:
                logger.warning("Rate limited, waiting 60 seconds...")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

            children = data.get('data', {}).get('children', [])
            if not children:
                break

            for child in children:
                post_data = child.get('data', {})
                posts.append({
                    'post_id': post_data.get('id', ''),
                    'title': post_data.get('title', ''),
                    'text': post_data.get('selftext', ''),
                    'author': post_data.get('author', ''),
                    'subreddit': subreddit,
                    'score': post_data.get('score', 0),
                    'upvote_ratio': post_data.get('upvote_ratio', 0),
                    'num_comments': post_data.get('num_comments', 0),
                    'created_utc': post_data.get('created_utc', 0),
                    'url': f"https://reddit.com{post_data.get('permalink', '')}",
                    'is_video': post_data.get('is_video', False),
                    'over_18': post_data.get('over_18', False),
                })

            fetched += len(children)
            after = data.get('data', {}).get('after')

            if not after:
                break

            time.sleep(2)  # Rate limiting

        except Exception as e:
            logger.warning(f"Error fetching r/{subreddit}: {str(e)}")
            break

    return posts


def search_reddit(query: str, limit: int = 100) -> List[Dict]:
    """
    Search Reddit for posts matching a query.

    Args:
        query: Search query
        limit: Max posts to fetch

    Returns:
        List of post dictionaries
    """
    posts = []
    after = None
    fetched = 0

    while fetched < limit:
        try:
            url = "https://www.reddit.com/search.json"
            params = {
                'q': query,
                'sort': 'relevance',
                'limit': min(100, limit - fetched),
                't': 'year'  # Last year
            }
            if after:
                params['after'] = after

            response = requests.get(url, headers=HEADERS, params=params, timeout=30)

            if response.status_code == 429:
                logger.warning("Rate limited, waiting 60 seconds...")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

            children = data.get('data', {}).get('children', [])
            if not children:
                break

            for child in children:
                post_data = child.get('data', {})
                posts.append({
                    'post_id': post_data.get('id', ''),
                    'title': post_data.get('title', ''),
                    'text': post_data.get('selftext', ''),
                    'author': post_data.get('author', ''),
                    'subreddit': post_data.get('subreddit', ''),
                    'score': post_data.get('score', 0),
                    'upvote_ratio': post_data.get('upvote_ratio', 0),
                    'num_comments': post_data.get('num_comments', 0),
                    'created_utc': post_data.get('created_utc', 0),
                    'url': f"https://reddit.com{post_data.get('permalink', '')}",
                    'search_query': query,
                })

            fetched += len(children)
            after = data.get('data', {}).get('after')

            if not after:
                break

            time.sleep(2)  # Rate limiting

        except Exception as e:
            logger.warning(f"Error searching '{query}': {str(e)}")
            break

    return posts


def is_relevant(title: str, text: str) -> bool:
    """Check if post is relevant based on keywords."""
    combined = f"{title} {text}".lower()
    return any(keyword in combined for keyword in RELEVANT_KEYWORDS)


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


def main(max_posts_per_sub: int = 100, max_search_results: int = 50):
    """
    Main function to scrape Reddit.

    Args:
        max_posts_per_sub: Max posts per subreddit
        max_search_results: Max results per search query
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("REDDIT SCRAPER (Public JSON API)")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Subreddits: {SUBREDDITS}")
    logger.info(f"Search queries: {len(SEARCH_QUERIES)}")
    logger.info(f"Max posts per subreddit: {max_posts_per_sub}")

    all_posts = []
    seen_ids = set()

    # Scrape subreddits
    logger.info("\n--- Scraping Subreddits ---")
    for subreddit in tqdm(SUBREDDITS, desc="Subreddits"):
        posts = fetch_subreddit_posts(subreddit, 'hot', max_posts_per_sub)

        for post in posts:
            if post['post_id'] not in seen_ids:
                seen_ids.add(post['post_id'])
                post['source'] = 'reddit_subreddit'
                post['platform'] = 'reddit'
                post['scraped_at'] = datetime.now().isoformat()
                all_posts.append(post)

        logger.info(f"  r/{subreddit}: {len(posts)} posts")
        time.sleep(3)  # Rate limiting

    # Search queries
    logger.info("\n--- Searching Reddit ---")
    for query in tqdm(SEARCH_QUERIES, desc="Searching"):
        posts = search_reddit(query, max_search_results)

        new_count = 0
        for post in posts:
            if post['post_id'] not in seen_ids:
                seen_ids.add(post['post_id'])
                post['source'] = 'reddit_search'
                post['platform'] = 'reddit'
                post['scraped_at'] = datetime.now().isoformat()
                all_posts.append(post)
                new_count += 1

        logger.info(f"  '{query}': {new_count} new posts")
        time.sleep(3)  # Rate limiting

    if not all_posts:
        logger.error("No posts scraped")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_posts)

    # Combine title and text for analysis
    df['review_text'] = df['title'] + ' ' + df['text'].fillna('')

    # Add relevance flag
    df['is_relevant'] = df.apply(lambda x: is_relevant(x['title'], x['text']), axis=1)

    # Add keyword categories
    categories = df['review_text'].apply(categorize_keywords)
    for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
        df[cat] = categories.apply(lambda x: x.get(cat, False))

    # Filter relevant
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'reddit_posts_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'reddit_posts_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("REDDIT SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total posts: {len(df)}")
    logger.info(f"Relevant posts: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}%)")

    logger.info("\nBy Subreddit (top 10):")
    sub_counts = df['subreddit'].value_counts().head(10)
    for sub, count in sub_counts.items():
        relevant = len(df[(df['subreddit'] == sub) & (df['is_relevant'])])
        logger.info(f"  r/{sub}: {count} total, {relevant} relevant")

    if len(relevant_df) > 0:
        logger.info("\nKeyword Categories in Relevant Posts:")
        for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
            count = relevant_df[cat].sum()
            pct = count / len(relevant_df) * 100
            logger.info(f"  {cat}: {count} ({pct:.1f}%)")

    # Show sample relevant posts
    if len(relevant_df) > 0:
        logger.info("\nSample Relevant Posts:")
        for _, row in relevant_df.head(3).iterrows():
            logger.info(f"\n  [r/{row['subreddit']}] {row['title'][:60]}...")
            logger.info(f"    Score: {row['score']}, Comments: {row['num_comments']}")

    logger.info(f"\nSaved to:")
    logger.info(f"  All posts: {all_path}")
    logger.info(f"  Relevant posts: {relevant_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Reddit posts")
    parser.add_argument('--max-posts', type=int, default=100,
                       help='Max posts per subreddit (default: 100)')
    parser.add_argument('--max-search', type=int, default=50,
                       help='Max results per search query (default: 50)')

    args = parser.parse_args()
    main(max_posts_per_sub=args.max_posts, max_search_results=args.max_search)
