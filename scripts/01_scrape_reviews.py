"""
The 'For You' Page Problem: Data Collection Script
===================================================
Scrapes app reviews from Google Play Store and Apple App Store
for TikTok, Instagram, YouTube, and Facebook.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# App package names for Google Play Store
PLAY_STORE_APPS = {
    'tiktok': 'com.zhiliaoapp.musically',
    'instagram': 'com.instagram.android',
    'youtube': 'com.google.android.youtube',
    'facebook': 'com.facebook.katana'
}

# App IDs for Apple App Store
APP_STORE_APPS = {
    'tiktok': '835599320',
    'instagram': '389801252',
    'youtube': '544007664',
    'facebook': '284882215'
}

# Countries to scrape (focus on Nepal, but include others for comparison)
COUNTRIES = ['np', 'in', 'us', 'gb']  # Nepal, India, US, UK

# Keywords related to explicit content, mental health, and age concerns
KEYWORDS = {
    'explicit_content': [
        'explicit', 'inappropriate', 'nude', 'nudity', 'sexual', 'sex',
        '18+', 'adult', 'vulgar', 'pornography', 'porn', 'naked',
        'revealing', 'suggestive', 'obscene', 'indecent', 'provocative',
        'nsfw', 'mature content', 'adult content', 'sexy', 'hot girls',
        'thirst trap', 'onlyfans'
    ],
    'age_concerns': [
        'kids', 'children', 'child', 'teenager', 'teen', 'young',
        'minor', 'underage', 'my child', 'my kids', 'my son', 'my daughter',
        'youth', 'adolescent', 'school', 'student', 'parental',
        '13', '14', '15', '16', 'years old'
    ],
    'mental_health': [
        'addicted', 'addiction', 'depressed', 'depression', 'anxiety',
        'anxious', 'mental health', 'compare myself', 'body image',
        'insecure', 'self-esteem', 'eating disorder', 'suicide',
        'lonely', 'loneliness', 'waste time', 'cant stop', "can't stop",
        'hours', 'all day', 'obsessed'
    ],
    'algorithm': [
        'fyp', 'for you page', 'algorithm', 'recommended', 'keeps showing',
        'suggestions', 'feed', 'explore', 'discover', 'personalized'
    ],
    'parental_controls': [
        'parental control', 'restrict', 'filter', 'block', 'age verification',
        'family safety', 'screen time', 'digital wellbeing', 'restricted mode'
    ]
}

# Output directory
OUTPUT_DIR = '../data/raw'

# ============================================================================
# GOOGLE PLAY STORE SCRAPER
# ============================================================================

def scrape_play_store(app_name: str, app_id: str, country: str = 'np',
                      max_reviews: int = 10000) -> pd.DataFrame:
    """
    Scrape reviews from Google Play Store.

    Args:
        app_name: Name of the app (for logging)
        app_id: Google Play Store package ID
        country: Country code (e.g., 'np' for Nepal)
        max_reviews: Maximum number of reviews to scrape

    Returns:
        DataFrame with review data
    """
    try:
        from google_play_scraper import Sort, reviews, reviews_all
    except ImportError:
        logger.error("google-play-scraper not installed. Run: pip install google-play-scraper")
        return pd.DataFrame()

    logger.info(f"Scraping Play Store reviews for {app_name} ({country})...")

    try:
        # Try to get all reviews (may be slow for popular apps)
        result, continuation_token = reviews(
            app_id,
            lang='en',
            country=country,
            sort=Sort.NEWEST,
            count=max_reviews
        )

        if not result:
            logger.warning(f"No reviews found for {app_name} in {country}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(result)

        # Add metadata
        df['app_name'] = app_name
        df['platform'] = 'google_play'
        df['country'] = country
        df['scraped_at'] = datetime.now().isoformat()

        # Rename columns for consistency
        column_mapping = {
            'content': 'review_text',
            'score': 'rating',
            'at': 'review_date',
            'userName': 'username',
            'reviewId': 'review_id',
            'thumbsUpCount': 'helpful_count'
        }
        df = df.rename(columns=column_mapping)

        # Select relevant columns
        cols_to_keep = ['review_id', 'username', 'review_text', 'rating',
                        'review_date', 'helpful_count', 'app_name', 'platform',
                        'country', 'scraped_at']
        df = df[[c for c in cols_to_keep if c in df.columns]]

        logger.info(f"Scraped {len(df)} reviews for {app_name} ({country})")
        return df

    except Exception as e:
        logger.error(f"Error scraping {app_name} from Play Store: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# APPLE APP STORE SCRAPER
# ============================================================================

def scrape_app_store(app_name: str, app_id: str, country: str = 'np',
                     max_reviews: int = 5000) -> pd.DataFrame:
    """
    Scrape reviews from Apple App Store.

    Args:
        app_name: Name of the app (for logging)
        app_id: Apple App Store app ID
        country: Country code
        max_reviews: Maximum number of reviews to scrape

    Returns:
        DataFrame with review data
    """
    try:
        from app_store_scraper import AppStore
    except ImportError:
        logger.error("app-store-scraper not installed. Run: pip install app-store-scraper")
        return pd.DataFrame()

    logger.info(f"Scraping App Store reviews for {app_name} ({country})...")

    try:
        app = AppStore(country=country, app_name=app_name, app_id=app_id)
        app.review(how_many=max_reviews)

        if not app.reviews:
            logger.warning(f"No reviews found for {app_name} in App Store ({country})")
            return pd.DataFrame()

        df = pd.DataFrame(app.reviews)

        # Add metadata
        df['app_name'] = app_name
        df['platform'] = 'app_store'
        df['country'] = country
        df['scraped_at'] = datetime.now().isoformat()

        # Rename columns
        column_mapping = {
            'review': 'review_text',
            'rating': 'rating',
            'date': 'review_date',
            'userName': 'username',
            'title': 'review_title'
        }
        df = df.rename(columns=column_mapping)

        logger.info(f"Scraped {len(df)} reviews for {app_name} from App Store ({country})")
        return df

    except Exception as e:
        logger.error(f"Error scraping {app_name} from App Store: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# KEYWORD FILTERING
# ============================================================================

def contains_keywords(text: str, keyword_categories: Dict[str, List[str]]) -> Dict[str, bool]:
    """
    Check if text contains keywords from each category.

    Args:
        text: Review text to check
        keyword_categories: Dictionary of category -> keywords

    Returns:
        Dictionary of category -> bool indicating if any keyword was found
    """
    if not isinstance(text, str):
        return {cat: False for cat in keyword_categories}

    text_lower = text.lower()
    results = {}

    for category, keywords in keyword_categories.items():
        results[category] = any(kw.lower() in text_lower for kw in keywords)

    return results


def filter_relevant_reviews(df: pd.DataFrame,
                           keyword_categories: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Filter reviews that contain relevant keywords and add category flags.

    Args:
        df: DataFrame with review_text column
        keyword_categories: Dictionary of category -> keywords

    Returns:
        Filtered DataFrame with category flags
    """
    if df.empty or 'review_text' not in df.columns:
        return df

    logger.info(f"Filtering {len(df)} reviews for relevant keywords...")

    # Add keyword category flags
    for category in keyword_categories:
        df[f'has_{category}'] = False

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Checking keywords"):
        flags = contains_keywords(row['review_text'], keyword_categories)
        for category, has_keyword in flags.items():
            df.at[idx, f'has_{category}'] = has_keyword

    # Create overall relevance flag
    category_cols = [f'has_{cat}' for cat in keyword_categories]
    df['is_relevant'] = df[category_cols].any(axis=1)

    relevant_count = df['is_relevant'].sum()
    logger.info(f"Found {relevant_count} relevant reviews ({relevant_count/len(df)*100:.1f}%)")

    return df


# ============================================================================
# MAIN SCRAPING FUNCTION
# ============================================================================

def scrape_all_platforms(output_dir: str = OUTPUT_DIR,
                        countries: List[str] = COUNTRIES,
                        max_reviews_per_app: int = 10000) -> pd.DataFrame:
    """
    Scrape reviews from all platforms and countries.

    Args:
        output_dir: Directory to save raw data
        countries: List of country codes to scrape
        max_reviews_per_app: Maximum reviews per app per country

    Returns:
        Combined DataFrame with all reviews
    """
    os.makedirs(output_dir, exist_ok=True)
    all_reviews = []

    # Scrape Google Play Store
    logger.info("=" * 50)
    logger.info("SCRAPING GOOGLE PLAY STORE")
    logger.info("=" * 50)

    for app_name, app_id in PLAY_STORE_APPS.items():
        for country in countries:
            df = scrape_play_store(app_name, app_id, country, max_reviews_per_app)
            if not df.empty:
                all_reviews.append(df)
            time.sleep(2)  # Rate limiting

    # Scrape Apple App Store
    logger.info("=" * 50)
    logger.info("SCRAPING APPLE APP STORE")
    logger.info("=" * 50)

    for app_name, app_id in APP_STORE_APPS.items():
        for country in countries:
            df = scrape_app_store(app_name, app_id, country, max_reviews_per_app // 2)
            if not df.empty:
                all_reviews.append(df)
            time.sleep(2)  # Rate limiting

    if not all_reviews:
        logger.error("No reviews scraped!")
        return pd.DataFrame()

    # Combine all reviews
    combined_df = pd.concat(all_reviews, ignore_index=True)
    logger.info(f"Total reviews scraped: {len(combined_df)}")

    # Filter for relevant reviews
    combined_df = filter_relevant_reviews(combined_df, KEYWORDS)

    # Save raw data
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save all reviews
    all_reviews_path = os.path.join(output_dir, f'all_reviews_{timestamp}.csv')
    combined_df.to_csv(all_reviews_path, index=False, encoding='utf-8')
    logger.info(f"Saved all reviews to: {all_reviews_path}")

    # Save only relevant reviews
    relevant_df = combined_df[combined_df['is_relevant'] == True]
    relevant_path = os.path.join(output_dir, f'relevant_reviews_{timestamp}.csv')
    relevant_df.to_csv(relevant_path, index=False, encoding='utf-8')
    logger.info(f"Saved {len(relevant_df)} relevant reviews to: {relevant_path}")

    # Print summary statistics
    print_summary(combined_df)

    return combined_df


def print_summary(df: pd.DataFrame):
    """Print summary statistics of scraped data."""
    logger.info("\n" + "=" * 50)
    logger.info("SCRAPING SUMMARY")
    logger.info("=" * 50)

    if df.empty:
        logger.info("No data to summarize.")
        return

    # By app
    logger.info("\nReviews by App:")
    print(df['app_name'].value_counts())

    # By platform
    logger.info("\nReviews by Platform:")
    print(df['platform'].value_counts())

    # By country
    logger.info("\nReviews by Country:")
    print(df['country'].value_counts())

    # By keyword category (for relevant reviews)
    logger.info("\nReviews by Keyword Category:")
    for category in KEYWORDS:
        col = f'has_{category}'
        if col in df.columns:
            count = df[col].sum()
            logger.info(f"  {category}: {count} ({count/len(df)*100:.1f}%)")

    # Relevant reviews
    if 'is_relevant' in df.columns:
        relevant = df['is_relevant'].sum()
        logger.info(f"\nTotal Relevant Reviews: {relevant} ({relevant/len(df)*100:.1f}%)")


# ============================================================================
# QUICK TEST FUNCTION
# ============================================================================

def quick_test(app_name: str = 'tiktok', count: int = 100) -> pd.DataFrame:
    """
    Quick test to verify scraping works.

    Args:
        app_name: App to test ('tiktok', 'instagram', 'youtube', 'facebook')
        count: Number of reviews to scrape

    Returns:
        DataFrame with test results
    """
    logger.info(f"Running quick test: {app_name}, {count} reviews")

    app_id = PLAY_STORE_APPS.get(app_name)
    if not app_id:
        logger.error(f"Unknown app: {app_name}")
        return pd.DataFrame()

    df = scrape_play_store(app_name, app_id, 'np', count)

    if not df.empty:
        logger.info(f"\nSample review:")
        print(df['review_text'].iloc[0][:200] if len(df) > 0 else "No reviews")

        # Filter and show relevant
        df = filter_relevant_reviews(df, KEYWORDS)
        relevant = df[df['is_relevant'] == True]

        if not relevant.empty:
            logger.info(f"\nSample RELEVANT review:")
            print(relevant['review_text'].iloc[0][:300])

    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape app reviews for 'The For You Page Problem' research"
    )
    parser.add_argument('--test', action='store_true',
                       help='Run quick test with 100 reviews')
    parser.add_argument('--app', type=str, default='tiktok',
                       choices=['tiktok', 'instagram', 'youtube', 'facebook'],
                       help='App to scrape (for test mode)')
    parser.add_argument('--max-reviews', type=int, default=10000,
                       help='Maximum reviews per app per country')
    parser.add_argument('--countries', type=str, nargs='+', default=['np', 'in'],
                       help='Country codes to scrape')

    args = parser.parse_args()

    if args.test:
        df = quick_test(args.app, 100)
    else:
        df = scrape_all_platforms(
            max_reviews_per_app=args.max_reviews,
            countries=args.countries
        )

    logger.info("Done!")
