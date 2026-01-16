"""
The 'For You' Page Problem: Data Cleaning Pipeline
===================================================
Cleans and preprocesses scraped review data for analysis.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import re
import logging
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
from tqdm import tqdm

# NLP imports
try:
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except ImportError:
    print("NLTK not installed. Run: pip install nltk")

try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 42  # For reproducibility
except ImportError:
    print("langdetect not installed. Run: pip install langdetect")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_DIR = '../data/raw'
OUTPUT_DIR = '../data/processed'

# Age inference patterns
AGE_PATTERNS = {
    'explicit_age': [
        (r"i[' ]?(?:a)?m\s*(\d{1,2})\s*(?:years?\s*old|yo|yrs?)?", 'self'),
        (r"(\d{1,2})\s*(?:year[s]?\s*old|y/?o)", 'self'),
        (r"my\s*(?:son|daughter|child|kid)\s*(?:is\s*)?(\d{1,2})", 'child'),
    ],
    'role_indicators': {
        'teen': ['teenager', 'teen', 'high school', 'middle school', 'homework', 'my parents'],
        'young_adult': ['college', 'university', 'dorm', 'student', 'grad school', 'campus'],
        'adult': ['work', 'office', 'job', 'career', 'coworker', 'colleague', 'boss'],
        'parent': ['my kids', 'my children', 'my son', 'my daughter', 'as a parent', 'parenting']
    }
}

# Spam/bot patterns
SPAM_PATTERNS = [
    r'click\s*here',
    r'free\s*(?:money|coins|followers)',
    r'visit\s*my\s*(?:profile|page|channel)',
    r'(?:buy|get)\s*followers',
    r'(?:http|www\.)\S+',  # URLs (often spam)
    r'(.)\1{5,}',  # Repeated characters (aaaaaaa)
    r'earn\s*\$?\d+',
    r'make\s*money\s*(?:fast|online)',
]

# ============================================================================
# TEXT CLEANING FUNCTIONS
# ============================================================================

def clean_text(text: str) -> str:
    """
    Clean review text by removing noise while preserving meaning.

    Args:
        text: Raw review text

    Returns:
        Cleaned text
    """
    if not isinstance(text, str):
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove URLs
    text = re.sub(r'http\S+|www\.\S+', '', text)

    # Remove email addresses
    text = re.sub(r'\S+@\S+', '', text)

    # Remove mentions (@username)
    text = re.sub(r'@\w+', '', text)

    # Remove hashtags (but keep the word)
    text = re.sub(r'#(\w+)', r'\1', text)

    # Remove emojis (keep text only for analysis)
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub('', text)

    # Remove special characters but keep apostrophes
    text = re.sub(r"[^a-zA-Z0-9\s']", ' ', text)

    # Remove extra whitespace
    text = ' '.join(text.split())

    return text.strip()


def is_spam(text: str) -> bool:
    """
    Check if text is likely spam.

    Args:
        text: Review text

    Returns:
        True if spam, False otherwise
    """
    if not isinstance(text, str) or len(text) < 10:
        return True

    text_lower = text.lower()

    # Check spam patterns
    for pattern in SPAM_PATTERNS:
        if re.search(pattern, text_lower):
            return True

    # Check if mostly non-alphabetic
    alpha_ratio = len(re.findall(r'[a-zA-Z]', text)) / len(text) if text else 0
    if alpha_ratio < 0.5:
        return True

    return False


def detect_language(text: str) -> str:
    """
    Detect the language of the text.

    Args:
        text: Review text

    Returns:
        ISO language code (e.g., 'en', 'ne', 'hi')
    """
    try:
        if not text or len(text) < 10:
            return 'unknown'
        return detect(text)
    except Exception:
        return 'unknown'


# ============================================================================
# AGE INFERENCE
# ============================================================================

def infer_age_category(text: str) -> Tuple[str, Optional[int], str]:
    """
    Infer age category from review text.

    Args:
        text: Review text

    Returns:
        Tuple of (age_category, explicit_age, inference_method)
    """
    if not isinstance(text, str):
        return ('unknown', None, 'no_text')

    text_lower = text.lower()

    # First, try to find explicit age mentions
    for pattern, mention_type in AGE_PATTERNS['explicit_age']:
        match = re.search(pattern, text_lower)
        if match:
            try:
                age = int(match.group(1))
                if mention_type == 'self':
                    if 10 <= age <= 17:
                        return ('teen', age, 'explicit_self')
                    elif 18 <= age <= 24:
                        return ('young_adult', age, 'explicit_self')
                    elif 25 <= age <= 34:
                        return ('adult', age, 'explicit_self')
                    elif age >= 35:
                        return ('older_adult', age, 'explicit_self')
                elif mention_type == 'child':
                    # Reviewer is a parent discussing their child
                    return ('parent', None, 'has_child')
            except ValueError:
                pass

    # Check role indicators
    for role, keywords in AGE_PATTERNS['role_indicators'].items():
        if any(kw in text_lower for kw in keywords):
            return (role, None, 'role_indicator')

    return ('unknown', None, 'no_indicator')


# ============================================================================
# MAIN CLEANING PIPELINE
# ============================================================================

def clean_dataset(input_path: str, output_dir: str = OUTPUT_DIR) -> pd.DataFrame:
    """
    Clean and preprocess the scraped review dataset.

    Args:
        input_path: Path to raw CSV file
        output_dir: Directory to save cleaned data

    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path, encoding='utf-8')
    initial_count = len(df)
    logger.info(f"Loaded {initial_count} reviews")

    os.makedirs(output_dir, exist_ok=True)

    # Step 1: Remove duplicates
    logger.info("Step 1: Removing duplicates...")
    if 'review_id' in df.columns:
        df = df.drop_duplicates(subset=['review_id'])
    else:
        df = df.drop_duplicates(subset=['review_text'])
    logger.info(f"  After dedup: {len(df)} ({initial_count - len(df)} removed)")

    # Step 2: Remove empty/null reviews
    logger.info("Step 2: Removing empty reviews...")
    df = df[df['review_text'].notna()]
    df = df[df['review_text'].str.len() >= 10]
    logger.info(f"  After empty removal: {len(df)}")

    # Step 3: Remove spam
    logger.info("Step 3: Removing spam...")
    df['is_spam'] = df['review_text'].apply(is_spam)
    spam_count = df['is_spam'].sum()
    df = df[~df['is_spam']]
    logger.info(f"  Removed {spam_count} spam reviews, remaining: {len(df)}")

    # Step 4: Clean text
    logger.info("Step 4: Cleaning text...")
    df['review_text_clean'] = df['review_text'].apply(clean_text)

    # Step 5: Detect language
    logger.info("Step 5: Detecting language...")
    tqdm.pandas(desc="Detecting language")
    df['language'] = df['review_text'].progress_apply(detect_language)

    # Language distribution
    logger.info("  Language distribution:")
    print(df['language'].value_counts().head(10))

    # Step 6: Infer age category
    logger.info("Step 6: Inferring age categories...")
    tqdm.pandas(desc="Inferring age")
    age_results = df['review_text'].progress_apply(infer_age_category)
    df['age_category'] = age_results.apply(lambda x: x[0])
    df['explicit_age'] = age_results.apply(lambda x: x[1])
    df['age_inference_method'] = age_results.apply(lambda x: x[2])

    # Age category distribution
    logger.info("  Age category distribution:")
    print(df['age_category'].value_counts())

    # Step 7: Add word count
    logger.info("Step 7: Adding metadata...")
    df['word_count'] = df['review_text_clean'].apply(lambda x: len(x.split()) if x else 0)

    # Step 8: Ensure date format
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')

    # Step 9: Drop intermediate columns
    df = df.drop(columns=['is_spam'], errors='ignore')

    # Save cleaned data
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'cleaned_reviews_{timestamp}.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved cleaned data to: {output_path}")

    # Print final summary
    print_cleaning_summary(df, initial_count)

    return df


def print_cleaning_summary(df: pd.DataFrame, initial_count: int):
    """Print summary of cleaning process."""
    logger.info("\n" + "=" * 50)
    logger.info("CLEANING SUMMARY")
    logger.info("=" * 50)

    logger.info(f"Initial reviews: {initial_count}")
    logger.info(f"Final reviews: {len(df)}")
    logger.info(f"Removed: {initial_count - len(df)} ({(initial_count - len(df))/initial_count*100:.1f}%)")

    logger.info("\nBy App:")
    print(df['app_name'].value_counts())

    logger.info("\nBy Age Category:")
    print(df['age_category'].value_counts())

    logger.info("\nBy Language (top 5):")
    print(df['language'].value_counts().head())

    if 'is_relevant' in df.columns:
        relevant = df['is_relevant'].sum()
        logger.info(f"\nRelevant reviews: {relevant} ({relevant/len(df)*100:.1f}%)")


# ============================================================================
# FILTER FUNCTIONS
# ============================================================================

def filter_by_relevance(df: pd.DataFrame,
                       require_explicit: bool = False,
                       require_age: bool = False,
                       require_mental_health: bool = False) -> pd.DataFrame:
    """
    Filter dataset by specific relevance criteria.

    Args:
        df: Cleaned DataFrame
        require_explicit: Require explicit content mention
        require_age: Require age-related mention
        require_mental_health: Require mental health mention

    Returns:
        Filtered DataFrame
    """
    filtered = df.copy()

    if require_explicit and 'has_explicit_content' in df.columns:
        filtered = filtered[filtered['has_explicit_content'] == True]

    if require_age and 'has_age_concerns' in df.columns:
        filtered = filtered[filtered['has_age_concerns'] == True]

    if require_mental_health and 'has_mental_health' in df.columns:
        filtered = filtered[filtered['has_mental_health'] == True]

    logger.info(f"Filtered to {len(filtered)} reviews")
    return filtered


def create_analysis_subsets(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Create analysis-ready subsets of the data.

    Args:
        df: Cleaned DataFrame
        output_dir: Directory to save subsets
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d')

    # Subset 1: All relevant reviews
    if 'is_relevant' in df.columns:
        relevant = df[df['is_relevant'] == True]
        relevant.to_csv(os.path.join(output_dir, f'subset_relevant_{timestamp}.csv'),
                       index=False, encoding='utf-8')
        logger.info(f"Saved relevant subset: {len(relevant)} reviews")

    # Subset 2: Reviews with known age
    known_age = df[df['age_category'] != 'unknown']
    known_age.to_csv(os.path.join(output_dir, f'subset_known_age_{timestamp}.csv'),
                    index=False, encoding='utf-8')
    logger.info(f"Saved known-age subset: {len(known_age)} reviews")

    # Subset 3: By platform
    for app in df['app_name'].unique():
        app_df = df[df['app_name'] == app]
        app_df.to_csv(os.path.join(output_dir, f'subset_{app}_{timestamp}.csv'),
                     index=False, encoding='utf-8')
        logger.info(f"Saved {app} subset: {len(app_df)} reviews")

    # Subset 4: English only (for easier NLP)
    english = df[df['language'] == 'en']
    english.to_csv(os.path.join(output_dir, f'subset_english_{timestamp}.csv'),
                  index=False, encoding='utf-8')
    logger.info(f"Saved English subset: {len(english)} reviews")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser(
        description="Clean scraped review data for 'The For You Page Problem' research"
    )
    parser.add_argument('--input', type=str,
                       help='Path to input CSV file (or use latest in data/raw)')
    parser.add_argument('--create-subsets', action='store_true',
                       help='Create analysis subsets after cleaning')

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = args.input
    else:
        # Find most recent file in raw directory
        raw_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
        if not raw_files:
            logger.error(f"No CSV files found in {INPUT_DIR}")
            exit(1)
        input_path = max(raw_files, key=os.path.getctime)
        logger.info(f"Using most recent file: {input_path}")

    # Clean the data
    df = clean_dataset(input_path)

    # Create subsets if requested
    if args.create_subsets:
        create_analysis_subsets(df)

    logger.info("Done!")
