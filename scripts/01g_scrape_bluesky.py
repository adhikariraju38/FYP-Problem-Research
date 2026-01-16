"""
Bluesky Scraper for 'The For You' Page Problem Research
========================================================
Scrapes posts from Bluesky (Twitter alternative) using public API.
NO API KEY REQUIRED - uses public search API.

Usage:
    python 01g_scrape_bluesky.py
    python 01g_scrape_bluesky.py --max-posts 200
"""

import os
import sys
import logging
import time
from datetime import datetime
from typing import List, Dict
from urllib.parse import quote

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

# Bluesky API endpoint
BLUESKY_API = "https://public.api.bsky.app"

# Search queries
SEARCH_QUERIES = [
    # TikTok concerns
    'tiktok inappropriate',
    'tiktok kids',
    'tiktok children',
    'tiktok dangerous',
    'tiktok addiction',
    'tiktok algorithm',
    'tiktok ban',
    'tiktok mental health',

    # Instagram Reels
    'instagram reels inappropriate',
    'reels algorithm',

    # YouTube Shorts
    'youtube shorts kids',
    'shorts inappropriate',

    # General social media
    'social media addiction',
    'social media children',
    'social media mental health',
    'algorithm problem',
    'fyp inappropriate',
]

# Keywords to identify relevant posts
RELEVANT_KEYWORDS = [
    'inappropriate', 'explicit', 'sexual', 'nudity', 'nude', '18+', 'adult',
    'vulgar', 'pornography', 'porn', 'naked', 'nsfw',
    'kid', 'child', 'children', 'teen', 'teenager', 'minor', 'young',
    'daughter', 'son', 'parent', 'underage',
    'addiction', 'addicted', 'mental health', 'depression', 'anxiety',
    'brain rot', 'attention span', 'dopamine', 'toxic', 'harmful',
    'algorithm', 'fyp', 'for you', 'recommended', 'keeps showing',
    'dangerous', 'ban', 'restrict', 'filter', 'parental', 'control',
    'protect', 'safety', 'block',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
}


def search_bluesky(query: str, max_posts: int = 100) -> List[Dict]:
    """
    Search Bluesky for posts.

    Args:
        query: Search query
        max_posts: Maximum posts to fetch

    Returns:
        List of post dictionaries
    """
    posts = []
    cursor = None

    while len(posts) < max_posts:
        try:
            url = f"{BLUESKY_API}/xrpc/app.bsky.feed.searchPosts"
            params = {
                'q': query,
                'limit': min(100, max_posts - len(posts)),
            }
            if cursor:
                params['cursor'] = cursor

            response = requests.get(url, headers=HEADERS, params=params, timeout=30)

            if response.status_code != 200:
                logger.debug(f"Error {response.status_code} for query: {query}")
                break

            data = response.json()
            feed_posts = data.get('posts', [])

            if not feed_posts:
                break

            for post in feed_posts:
                record = post.get('record', {})
                author = post.get('author', {})

                posts.append({
                    'post_id': post.get('uri', '').split('/')[-1],
                    'text': record.get('text', ''),
                    'author_handle': author.get('handle', ''),
                    'author_name': author.get('displayName', ''),
                    'created_at': record.get('createdAt', ''),
                    'likes': post.get('likeCount', 0),
                    'reposts': post.get('repostCount', 0),
                    'replies': post.get('replyCount', 0),
                    'search_query': query,
                })

            cursor = data.get('cursor')
            if not cursor:
                break

            time.sleep(1)  # Rate limiting

        except Exception as e:
            logger.debug(f"Error searching Bluesky for '{query}': {str(e)}")
            break

    return posts


def is_relevant(text: str) -> bool:
    """Check if post text contains relevant keywords."""
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


def main(max_posts_per_query: int = 100):
    """
    Main function to scrape Bluesky.

    Args:
        max_posts_per_query: Max posts per search query
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("BLUESKY SCRAPER (Public API)")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Search queries: {len(SEARCH_QUERIES)}")
    logger.info(f"Max posts per query: {max_posts_per_query}")

    all_posts = []
    seen_ids = set()

    for query in tqdm(SEARCH_QUERIES, desc="Searching Bluesky"):
        posts = search_bluesky(query, max_posts_per_query)

        new_count = 0
        for post in posts:
            post_id = post.get('post_id', '')
            if post_id and post_id not in seen_ids:
                seen_ids.add(post_id)
                post['source'] = 'bluesky'
                post['platform'] = 'bluesky'
                post['review_text'] = post['text']  # For compatibility
                post['scraped_at'] = datetime.now().isoformat()
                all_posts.append(post)
                new_count += 1

        logger.info(f"  '{query}': {new_count} new posts")
        time.sleep(2)  # Rate limiting

    if not all_posts:
        logger.error("No posts scraped")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_posts)

    # Add relevance flag
    df['is_relevant'] = df['text'].apply(is_relevant)

    # Add keyword categories
    categories = df['text'].apply(categorize_keywords)
    for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
        df[cat] = categories.apply(lambda x: x.get(cat, False))

    # Filter relevant
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'bluesky_posts_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'bluesky_posts_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("BLUESKY SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total posts: {len(df)}")
    logger.info(f"Relevant posts: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}%)")

    if len(df) > 0:
        logger.info(f"\nEngagement Stats:")
        logger.info(f"  Total likes: {df['likes'].sum()}")
        logger.info(f"  Total reposts: {df['reposts'].sum()}")

    if len(relevant_df) > 0:
        logger.info("\nKeyword Categories in Relevant Posts:")
        for cat in ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']:
            count = relevant_df[cat].sum()
            pct = count / len(relevant_df) * 100
            logger.info(f"  {cat}: {count} ({pct:.1f}%)")

        # Show sample posts
        logger.info("\nSample Relevant Posts:")
        for _, row in relevant_df.head(3).iterrows():
            logger.info(f"\n  @{row['author_handle']}: {row['text'][:80]}...")

    logger.info(f"\nSaved to:")
    logger.info(f"  All posts: {all_path}")
    logger.info(f"  Relevant posts: {relevant_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Bluesky posts")
    parser.add_argument('--max-posts', type=int, default=200,
                       help='Max posts per search query (default: 200)')

    args = parser.parse_args()
    main(max_posts_per_query=args.max_posts)
