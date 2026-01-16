"""
YouTube Comments Scraper for 'The For You' Page Problem Research
================================================================
Automatically searches for relevant videos and scrapes comments.
NO API KEY REQUIRED - uses scrapetube + youtube-comment-downloader.

Usage:
    python 01c_scrape_youtube.py
    python 01c_scrape_youtube.py --max-videos 20 --max-comments 500
"""

import os
import sys
import logging
import time
from datetime import datetime
from typing import List, Dict, Generator

import pandas as pd
from tqdm import tqdm

# Add system site-packages for scrapetube
sys.path.insert(0, '/var/data/python/lib/python3.13/site-packages')
sys.path.insert(0, '/home/rajuyadav/.local/lib/python3.13/site-packages')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = '../data/raw'

# Search queries to find relevant videos
SEARCH_QUERIES = [
    # TikTok explicit content concerns
    "tiktok inappropriate content kids",
    "tiktok dangerous for children",
    "tiktok explicit content problem",
    "tiktok algorithm showing inappropriate",
    "tiktok fyp problem",

    # Instagram Reels concerns
    "instagram reels inappropriate content",
    "reels showing explicit content",
    "instagram algorithm kids safety",

    # YouTube Shorts concerns
    "youtube shorts inappropriate content",
    "youtube shorts kids safety",

    # General social media mental health
    "social media addiction teenagers",
    "tiktok mental health effects",
    "short video addiction brain",
    "social media affecting youth",

    # Nepal specific
    "tiktok ban nepal",
    "social media nepal youth",

    # Parenting concerns
    "parental controls tiktok",
    "protecting kids from tiktok",
    "social media dangers children",
]

# Keywords to identify relevant comments
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


def search_youtube_videos(query: str, max_results: int = 10) -> List[Dict]:
    """
    Search YouTube for videos using scrapetube.

    Args:
        query: Search query string
        max_results: Maximum number of videos to return

    Returns:
        List of video dictionaries with id, title, etc.
    """
    try:
        import scrapetube
    except ImportError:
        logger.error("scrapetube not installed. Run: pip install scrapetube")
        return []

    videos = []
    try:
        logger.info(f"Searching: '{query}'")
        results = scrapetube.get_search(
            query,
            limit=max_results,
            sleep=1,  # Be nice to YouTube
            sort_by="relevance"
        )

        for video in results:
            videos.append({
                'video_id': video.get('videoId', ''),
                'title': video.get('title', {}).get('runs', [{}])[0].get('text', ''),
                'channel': video.get('ownerText', {}).get('runs', [{}])[0].get('text', ''),
                'view_count': video.get('viewCountText', {}).get('simpleText', ''),
                'length': video.get('lengthText', {}).get('simpleText', ''),
                'search_query': query
            })

        logger.info(f"  Found {len(videos)} videos")

    except Exception as e:
        logger.warning(f"Error searching for '{query}': {str(e)}")

    return videos


def scrape_video_comments(video_id: str, max_comments: int = 300) -> List[Dict]:
    """Scrape comments from a single YouTube video."""
    try:
        from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR
    except ImportError:
        logger.error("youtube-comment-downloader not installed. Run: pip install youtube-comment-downloader")
        return []

    downloader = YoutubeCommentDownloader()
    comments = []

    try:
        url = f'https://www.youtube.com/watch?v={video_id}'
        generator = downloader.get_comments_from_url(url, sort_by=SORT_BY_POPULAR)

        count = 0
        for comment in generator:
            if count >= max_comments:
                break

            comments.append({
                'comment_id': comment.get('cid', ''),
                'video_id': video_id,
                'text': comment.get('text', ''),
                'author': comment.get('author', ''),
                'likes': comment.get('votes', 0),
                'time': comment.get('time', ''),
                'reply_count': comment.get('replies', 0),
                'platform': 'youtube',
                'source': 'youtube_comments',
                'scraped_at': datetime.now().isoformat()
            })
            count += 1

    except Exception as e:
        logger.warning(f"Error scraping video {video_id}: {str(e)}")

    return comments


def is_relevant(text: str) -> bool:
    """Check if comment text contains relevant keywords."""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in RELEVANT_KEYWORDS)


def main(max_videos_per_query: int = 10, max_comments_per_video: int = 300):
    """
    Main function to search videos and scrape comments.

    Args:
        max_videos_per_query: Max videos to get per search query
        max_comments_per_video: Max comments per video
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("YOUTUBE COMMENTS SCRAPER (AUTO-SEARCH)")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)
    logger.info(f"Search queries: {len(SEARCH_QUERIES)}")
    logger.info(f"Max videos per query: {max_videos_per_query}")
    logger.info(f"Max comments per video: {max_comments_per_video}")

    # Step 1: Search for relevant videos
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: SEARCHING FOR VIDEOS")
    logger.info("=" * 60)

    all_videos = []
    seen_video_ids = set()

    for query in tqdm(SEARCH_QUERIES, desc="Searching"):
        videos = search_youtube_videos(query, max_videos_per_query)

        for video in videos:
            if video['video_id'] and video['video_id'] not in seen_video_ids:
                seen_video_ids.add(video['video_id'])
                all_videos.append(video)

        time.sleep(2)  # Rate limiting between searches

    logger.info(f"\nFound {len(all_videos)} unique videos")

    if not all_videos:
        logger.error("No videos found. Check your internet connection.")
        return

    # Save video list
    videos_df = pd.DataFrame(all_videos)
    videos_path = os.path.join(OUTPUT_DIR, f'youtube_videos_{timestamp}.csv')
    videos_df.to_csv(videos_path, index=False, encoding='utf-8')
    logger.info(f"Saved video list to: {videos_path}")

    # Step 2: Scrape comments from videos
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: SCRAPING COMMENTS")
    logger.info("=" * 60)

    all_comments = []
    videos_scraped = 0
    videos_with_comments = 0

    for video in tqdm(all_videos, desc="Scraping comments"):
        video_id = video['video_id']
        logger.info(f"\nScraping: {video['title'][:50]}...")

        comments = scrape_video_comments(video_id, max_comments_per_video)

        # Add video metadata to comments
        for comment in comments:
            comment['video_title'] = video.get('title', '')
            comment['search_query'] = video.get('search_query', '')

        all_comments.extend(comments)
        videos_scraped += 1

        if comments:
            videos_with_comments += 1
            logger.info(f"  Got {len(comments)} comments")
        else:
            logger.info(f"  No comments (disabled or error)")

        time.sleep(2)  # Rate limiting

    if not all_comments:
        logger.error("No comments scraped")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_comments)

    # Filter relevant
    df['is_relevant'] = df['text'].apply(is_relevant)
    relevant_df = df[df['is_relevant']].copy()

    # Save results
    all_path = os.path.join(OUTPUT_DIR, f'youtube_comments_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'youtube_comments_relevant_{timestamp}.csv')

    df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("YOUTUBE SCRAPING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Search queries used: {len(SEARCH_QUERIES)}")
    logger.info(f"Unique videos found: {len(all_videos)}")
    logger.info(f"Videos scraped: {videos_scraped}")
    logger.info(f"Videos with comments: {videos_with_comments}")
    logger.info(f"Total comments: {len(df)}")
    logger.info(f"Relevant comments: {len(relevant_df)} ({len(relevant_df)/len(df)*100:.1f}%)")
    logger.info(f"\nSaved to:")
    logger.info(f"  Videos: {videos_path}")
    logger.info(f"  All comments: {all_path}")
    logger.info(f"  Relevant comments: {relevant_path}")

    # Show sample of relevant comments
    if len(relevant_df) > 0:
        logger.info("\n" + "=" * 60)
        logger.info("SAMPLE RELEVANT COMMENTS")
        logger.info("=" * 60)
        for _, row in relevant_df.head(5).iterrows():
            logger.info(f"\n[{row.get('video_title', '')[:30]}...]")
            logger.info(f"  {row['text'][:200]}...")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape YouTube comments (auto-search)")
    parser.add_argument('--max-videos', type=int, default=10,
                       help='Max videos per search query (default: 10)')
    parser.add_argument('--max-comments', type=int, default=300,
                       help='Max comments per video (default: 300)')

    args = parser.parse_args()
    main(max_videos_per_query=args.max_videos, max_comments_per_video=args.max_comments)
