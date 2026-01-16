"""
Combine All Data Sources for 'The For You' Page Problem Research
=================================================================
Combines data from:
- Google Play Store reviews
- Apple App Store reviews
- Reddit posts
- Trustpilot reviews

Creates a unified dataset with standardized columns for analysis.

Usage:
    python 01h_combine_all_data.py
"""

import os
import glob
import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

INPUT_DIR = '../data/raw'
OUTPUT_DIR = '../data/processed'

# Standard columns for unified dataset
STANDARD_COLUMNS = [
    'id',                    # Unique identifier
    'text',                  # Main text content
    'source',                # Data source (playstore, appstore, reddit, trustpilot)
    'platform',              # Platform being discussed (tiktok, instagram, youtube, facebook)
    'rating',                # Rating if available (1-5)
    'author',                # Author/username
    'date',                  # Date of post/review
    'engagement_score',      # Likes/upvotes/helpful votes
    'is_relevant',           # Whether it matches research keywords
    'explicit_content',      # Mentions explicit/inappropriate content
    'age_concerns',          # Mentions children/teens/age
    'mental_health',         # Mentions addiction/mental health
    'algorithm',             # Mentions algorithm/fyp
    'parental_controls',     # Mentions parental controls/safety
    'scraped_at',            # When data was collected
]


def load_playstore_data() -> pd.DataFrame:
    """Load and standardize Play Store reviews."""
    files = glob.glob(os.path.join(INPUT_DIR, 'all_reviews_*.csv'))
    if not files:
        logger.warning("No Play Store files found")
        return pd.DataFrame()

    # Get the largest/most recent file
    latest = max(files, key=os.path.getsize)
    logger.info(f"Loading Play Store data from: {latest}")

    df = pd.read_csv(latest, encoding='utf-8')
    logger.info(f"  Loaded {len(df)} Play Store reviews")

    # Standardize columns
    standardized = pd.DataFrame()
    standardized['id'] = df.get('review_id', df.index.astype(str))
    standardized['text'] = df.get('review_text', df.get('content', ''))
    standardized['source'] = 'playstore'
    standardized['platform'] = df.get('app_name', df.get('platform', 'unknown'))
    standardized['rating'] = df.get('rating', df.get('score', 0))
    standardized['author'] = df.get('author', df.get('userName', ''))
    standardized['date'] = df.get('review_date', df.get('at', ''))
    standardized['engagement_score'] = df.get('thumbs_up', df.get('thumbsUpCount', 0))
    standardized['is_relevant'] = df.get('is_relevant', False)
    standardized['explicit_content'] = df.get('explicit_content', df.get('has_explicit_content', False))
    standardized['age_concerns'] = df.get('age_concerns', df.get('has_age_concerns', False))
    standardized['mental_health'] = df.get('mental_health', df.get('has_mental_health', False))
    standardized['algorithm'] = df.get('algorithm', df.get('has_algorithm', False))
    standardized['parental_controls'] = df.get('parental_controls', df.get('has_parental_controls', False))
    standardized['scraped_at'] = df.get('scraped_at', datetime.now().isoformat())

    return standardized


def load_appstore_data() -> pd.DataFrame:
    """Load and standardize App Store reviews."""
    files = glob.glob(os.path.join(INPUT_DIR, 'appstore_reviews_all_*.csv'))
    if not files:
        logger.warning("No App Store files found")
        return pd.DataFrame()

    latest = max(files, key=os.path.getctime)
    logger.info(f"Loading App Store data from: {latest}")

    df = pd.read_csv(latest, encoding='utf-8')
    logger.info(f"  Loaded {len(df)} App Store reviews")

    # Standardize columns
    standardized = pd.DataFrame()
    standardized['id'] = df.get('review_id', df.index.astype(str))
    standardized['text'] = df.get('review_text', '')
    standardized['source'] = 'appstore'
    standardized['platform'] = df.get('app_name', df.get('platform', 'unknown'))
    standardized['rating'] = df.get('rating', 0)
    standardized['author'] = df.get('author', '')
    standardized['date'] = df.get('date', '')
    standardized['engagement_score'] = df.get('vote_sum', df.get('vote_count', 0))
    standardized['is_relevant'] = df.get('is_relevant', False)
    standardized['explicit_content'] = df.get('explicit_content', False)
    standardized['age_concerns'] = df.get('age_concerns', False)
    standardized['mental_health'] = df.get('mental_health', False)
    standardized['algorithm'] = df.get('algorithm', False)
    standardized['parental_controls'] = df.get('parental_controls', False)
    standardized['scraped_at'] = df.get('scraped_at', datetime.now().isoformat())

    return standardized


def load_reddit_data() -> pd.DataFrame:
    """Load and standardize Reddit posts."""
    files = glob.glob(os.path.join(INPUT_DIR, 'reddit_posts_all_*.csv'))
    if not files:
        logger.warning("No Reddit files found")
        return pd.DataFrame()

    latest = max(files, key=os.path.getctime)
    logger.info(f"Loading Reddit data from: {latest}")

    df = pd.read_csv(latest, encoding='utf-8')
    logger.info(f"  Loaded {len(df)} Reddit posts")

    # Standardize columns
    standardized = pd.DataFrame()
    standardized['id'] = df.get('post_id', df.index.astype(str))
    standardized['text'] = df.get('review_text', df.get('title', '') + ' ' + df.get('text', '').fillna(''))
    standardized['source'] = 'reddit'

    # Infer platform from subreddit or content
    def infer_platform(row):
        subreddit = str(row.get('subreddit', '')).lower()
        text = str(row.get('review_text', '')).lower()
        if 'tiktok' in subreddit or 'tiktok' in text:
            return 'tiktok'
        elif 'instagram' in subreddit or 'instagram' in text or 'reels' in text:
            return 'instagram'
        elif 'youtube' in subreddit or 'youtube' in text or 'shorts' in text:
            return 'youtube'
        elif 'facebook' in subreddit or 'facebook' in text:
            return 'facebook'
        return 'social_media_general'

    standardized['platform'] = df.apply(infer_platform, axis=1)
    standardized['rating'] = 0  # Reddit doesn't have ratings
    standardized['author'] = df.get('author', '')
    standardized['date'] = pd.to_datetime(df.get('created_utc', 0), unit='s', errors='coerce')
    standardized['engagement_score'] = df.get('score', 0)
    standardized['is_relevant'] = df.get('is_relevant', False)
    standardized['explicit_content'] = df.get('explicit_content', False)
    standardized['age_concerns'] = df.get('age_concerns', False)
    standardized['mental_health'] = df.get('mental_health', False)
    standardized['algorithm'] = df.get('algorithm', False)
    standardized['parental_controls'] = df.get('parental_controls', False)
    standardized['scraped_at'] = df.get('scraped_at', datetime.now().isoformat())

    return standardized


def load_trustpilot_data() -> pd.DataFrame:
    """Load and standardize Trustpilot reviews."""
    files = glob.glob(os.path.join(INPUT_DIR, 'trustpilot_reviews_all_*.csv'))
    if not files:
        logger.warning("No Trustpilot files found")
        return pd.DataFrame()

    latest = max(files, key=os.path.getctime)
    logger.info(f"Loading Trustpilot data from: {latest}")

    df = pd.read_csv(latest, encoding='utf-8')
    logger.info(f"  Loaded {len(df)} Trustpilot reviews")

    # Standardize columns
    standardized = pd.DataFrame()
    standardized['id'] = 'tp_' + df.index.astype(str)
    standardized['text'] = df.get('review_text', '')
    standardized['source'] = 'trustpilot'
    standardized['platform'] = df.get('app_name', df.get('platform', 'unknown'))
    standardized['rating'] = df.get('rating', 0)
    standardized['author'] = df.get('author', '')
    standardized['date'] = df.get('date', '')
    standardized['engagement_score'] = 0  # Trustpilot doesn't expose this
    standardized['is_relevant'] = df.get('is_relevant', False)
    standardized['explicit_content'] = df.get('explicit_content', False)
    standardized['age_concerns'] = df.get('age_concerns', False)
    standardized['mental_health'] = df.get('mental_health', False)
    standardized['algorithm'] = df.get('algorithm', False)
    standardized['parental_controls'] = df.get('parental_controls', False)
    standardized['scraped_at'] = df.get('scraped_at', datetime.now().isoformat())

    return standardized


def load_youtube_data() -> pd.DataFrame:
    """Load and standardize YouTube comments."""
    files = glob.glob(os.path.join(INPUT_DIR, 'youtube_comments_all_*.csv'))
    if not files:
        logger.warning("No YouTube comments files found")
        return pd.DataFrame()

    latest = max(files, key=os.path.getctime)
    logger.info(f"Loading YouTube data from: {latest}")

    df = pd.read_csv(latest, encoding='utf-8')
    logger.info(f"  Loaded {len(df)} YouTube comments")

    # Standardize columns
    standardized = pd.DataFrame()
    standardized['id'] = df.get('comment_id', df.index.astype(str))
    standardized['text'] = df.get('text', df.get('comment', ''))
    standardized['source'] = 'youtube'

    # Infer platform from video title or search query
    def infer_platform(row):
        title = str(row.get('video_title', '')).lower()
        query = str(row.get('search_query', '')).lower()
        combined = title + ' ' + query
        if 'tiktok' in combined:
            return 'tiktok'
        elif 'instagram' in combined or 'reels' in combined:
            return 'instagram'
        elif 'youtube' in combined or 'shorts' in combined:
            return 'youtube'
        elif 'facebook' in combined:
            return 'facebook'
        return 'social_media_general'

    standardized['platform'] = df.apply(infer_platform, axis=1)
    standardized['rating'] = 0  # YouTube comments don't have ratings
    standardized['author'] = df.get('author', '')
    standardized['date'] = df.get('time', df.get('published_at', ''))
    standardized['engagement_score'] = df.get('votes', df.get('likes', 0))
    standardized['is_relevant'] = df.get('is_relevant', False)
    standardized['explicit_content'] = df.get('explicit_content', False)
    standardized['age_concerns'] = df.get('age_concerns', False)
    standardized['mental_health'] = df.get('mental_health', False)
    standardized['algorithm'] = df.get('algorithm', False)
    standardized['parental_controls'] = df.get('parental_controls', False)
    standardized['scraped_at'] = df.get('scraped_at', datetime.now().isoformat())

    return standardized


def main():
    """Combine all data sources into unified dataset."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logger.info("=" * 60)
    logger.info("COMBINING ALL DATA SOURCES")
    logger.info("The 'For You' Page Problem Research")
    logger.info("=" * 60)

    # Load all data sources
    dataframes = []

    logger.info("\n--- Loading Data Sources ---")

    # Play Store
    playstore_df = load_playstore_data()
    if len(playstore_df) > 0:
        dataframes.append(playstore_df)

    # App Store
    appstore_df = load_appstore_data()
    if len(appstore_df) > 0:
        dataframes.append(appstore_df)

    # Reddit
    reddit_df = load_reddit_data()
    if len(reddit_df) > 0:
        dataframes.append(reddit_df)

    # Trustpilot
    trustpilot_df = load_trustpilot_data()
    if len(trustpilot_df) > 0:
        dataframes.append(trustpilot_df)

    # YouTube
    youtube_df = load_youtube_data()
    if len(youtube_df) > 0:
        dataframes.append(youtube_df)

    if not dataframes:
        logger.error("No data sources found!")
        return

    # Combine all dataframes
    logger.info("\n--- Combining Data ---")
    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Total combined records: {len(combined_df)}")

    # Remove duplicates based on text (different sources might have same content)
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['text'], keep='first')
    logger.info(f"After deduplication: {len(combined_df)} ({initial_count - len(combined_df)} duplicates removed)")

    # Filter to relevant only
    relevant_df = combined_df[combined_df['is_relevant'] == True].copy()

    # Save combined data
    all_path = os.path.join(OUTPUT_DIR, f'combined_all_{timestamp}.csv')
    relevant_path = os.path.join(OUTPUT_DIR, f'combined_relevant_{timestamp}.csv')

    combined_df.to_csv(all_path, index=False, encoding='utf-8')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("COMBINATION COMPLETE")
    logger.info("=" * 60)

    logger.info(f"\nTotal records: {len(combined_df)}")
    logger.info(f"Relevant records: {len(relevant_df)} ({len(relevant_df)/len(combined_df)*100:.1f}%)")

    logger.info("\n--- By Source ---")
    source_counts = combined_df['source'].value_counts()
    for source, count in source_counts.items():
        relevant = len(combined_df[(combined_df['source'] == source) & (combined_df['is_relevant'] == True)])
        logger.info(f"  {source}: {count:,} total, {relevant:,} relevant")

    logger.info("\n--- By Platform ---")
    platform_counts = combined_df['platform'].value_counts()
    for platform, count in platform_counts.head(10).items():
        relevant = len(combined_df[(combined_df['platform'] == platform) & (combined_df['is_relevant'] == True)])
        logger.info(f"  {platform}: {count:,} total, {relevant:,} relevant")

    if len(relevant_df) > 0:
        logger.info("\n--- Keyword Categories (Relevant Data) ---")
        categories = ['explicit_content', 'age_concerns', 'mental_health', 'algorithm', 'parental_controls']
        for cat in categories:
            if cat in relevant_df.columns:
                count = relevant_df[cat].sum()
                pct = count / len(relevant_df) * 100
                logger.info(f"  {cat}: {count:,} ({pct:.1f}%)")

    logger.info(f"\n--- Output Files ---")
    logger.info(f"  All data: {all_path}")
    logger.info(f"  Relevant data: {relevant_path}")

    return combined_df, relevant_df


if __name__ == "__main__":
    main()
