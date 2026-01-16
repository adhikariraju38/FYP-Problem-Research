"""
The 'For You' Page Problem: Sentiment Analysis
===============================================
Performs sentiment analysis on cleaned review data.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np
from tqdm import tqdm

# Sentiment Analysis libraries
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    print("VADER not installed. Run: pip install vaderSentiment")

try:
    from textblob import TextBlob
except ImportError:
    print("TextBlob not installed. Run: pip install textblob")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

INPUT_DIR = '../data/processed'
OUTPUT_DIR = '../data/processed'

# Sentiment thresholds (VADER compound score)
SENTIMENT_THRESHOLDS = {
    'positive': 0.05,
    'negative': -0.05
}

# ============================================================================
# SENTIMENT ANALYZERS
# ============================================================================

def analyze_vader(text: str, analyzer: SentimentIntensityAnalyzer) -> Dict[str, float]:
    """
    Analyze sentiment using VADER.

    Args:
        text: Text to analyze
        analyzer: VADER analyzer instance

    Returns:
        Dictionary with sentiment scores
    """
    if not isinstance(text, str) or not text.strip():
        return {'neg': 0, 'neu': 1, 'pos': 0, 'compound': 0}

    scores = analyzer.polarity_scores(text)
    return scores


def analyze_textblob(text: str) -> Dict[str, float]:
    """
    Analyze sentiment using TextBlob.

    Args:
        text: Text to analyze

    Returns:
        Dictionary with polarity and subjectivity
    """
    if not isinstance(text, str) or not text.strip():
        return {'polarity': 0, 'subjectivity': 0}

    blob = TextBlob(text)
    return {
        'polarity': blob.sentiment.polarity,      # -1 to 1
        'subjectivity': blob.sentiment.subjectivity  # 0 to 1
    }


def categorize_sentiment(compound_score: float) -> str:
    """
    Categorize sentiment based on compound score.

    Args:
        compound_score: VADER compound score (-1 to 1)

    Returns:
        Sentiment category (positive, negative, neutral)
    """
    if compound_score >= SENTIMENT_THRESHOLDS['positive']:
        return 'positive'
    elif compound_score <= SENTIMENT_THRESHOLDS['negative']:
        return 'negative'
    else:
        return 'neutral'


# ============================================================================
# MAIN ANALYSIS FUNCTION
# ============================================================================

def run_sentiment_analysis(input_path: str, output_dir: str = OUTPUT_DIR) -> pd.DataFrame:
    """
    Run sentiment analysis on cleaned review data.

    Args:
        input_path: Path to cleaned CSV file
        output_dir: Directory to save results

    Returns:
        DataFrame with sentiment scores
    """
    logger.info(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path, encoding='utf-8')
    logger.info(f"Loaded {len(df)} reviews")

    os.makedirs(output_dir, exist_ok=True)

    # Initialize VADER
    vader = SentimentIntensityAnalyzer()

    # Determine which text column to use
    text_col = 'review_text_clean' if 'review_text_clean' in df.columns else 'review_text'

    # Run VADER sentiment analysis
    logger.info("Running VADER sentiment analysis...")
    tqdm.pandas(desc="VADER analysis")

    vader_results = df[text_col].progress_apply(lambda x: analyze_vader(x, vader))

    df['vader_neg'] = vader_results.apply(lambda x: x['neg'])
    df['vader_neu'] = vader_results.apply(lambda x: x['neu'])
    df['vader_pos'] = vader_results.apply(lambda x: x['pos'])
    df['vader_compound'] = vader_results.apply(lambda x: x['compound'])
    df['sentiment_category'] = df['vader_compound'].apply(categorize_sentiment)

    # Run TextBlob analysis
    logger.info("Running TextBlob sentiment analysis...")
    tqdm.pandas(desc="TextBlob analysis")

    textblob_results = df[text_col].progress_apply(analyze_textblob)

    df['textblob_polarity'] = textblob_results.apply(lambda x: x['polarity'])
    df['textblob_subjectivity'] = textblob_results.apply(lambda x: x['subjectivity'])

    # Create combined sentiment score (average of VADER compound and TextBlob polarity)
    df['sentiment_combined'] = (df['vader_compound'] + df['textblob_polarity']) / 2

    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'sentiment_analyzed_{timestamp}.csv')
    df.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Saved sentiment analysis to: {output_path}")

    # Print summary
    print_sentiment_summary(df)

    return df


def print_sentiment_summary(df: pd.DataFrame):
    """Print sentiment analysis summary."""
    logger.info("\n" + "=" * 50)
    logger.info("SENTIMENT ANALYSIS SUMMARY")
    logger.info("=" * 50)

    # Overall sentiment distribution
    logger.info("\nOverall Sentiment Distribution:")
    print(df['sentiment_category'].value_counts())
    print(f"\nPercentages:")
    print(df['sentiment_category'].value_counts(normalize=True).mul(100).round(1))

    # Average scores
    logger.info(f"\nAverage VADER Compound Score: {df['vader_compound'].mean():.3f}")
    logger.info(f"Average TextBlob Polarity: {df['textblob_polarity'].mean():.3f}")
    logger.info(f"Average TextBlob Subjectivity: {df['textblob_subjectivity'].mean():.3f}")

    # By app
    if 'app_name' in df.columns:
        logger.info("\nSentiment by App:")
        app_sentiment = df.groupby('app_name')['vader_compound'].agg(['mean', 'std', 'count'])
        print(app_sentiment.round(3))

    # By age category
    if 'age_category' in df.columns:
        logger.info("\nSentiment by Age Category:")
        age_sentiment = df.groupby('age_category')['vader_compound'].agg(['mean', 'std', 'count'])
        print(age_sentiment.round(3))

    # Sentiment by keyword categories
    keyword_cols = [c for c in df.columns if c.startswith('has_')]
    if keyword_cols:
        logger.info("\nSentiment by Keyword Category:")
        for col in keyword_cols:
            category = col.replace('has_', '')
            cat_sentiment = df[df[col] == True]['vader_compound'].mean()
            non_cat_sentiment = df[df[col] == False]['vader_compound'].mean()
            count = df[col].sum()
            logger.info(f"  {category}: {cat_sentiment:.3f} (n={count}) vs non-{category}: {non_cat_sentiment:.3f}")


# ============================================================================
# COMPARATIVE ANALYSIS
# ============================================================================

def compare_sentiment_groups(df: pd.DataFrame,
                            group_col: str,
                            sentiment_col: str = 'vader_compound') -> pd.DataFrame:
    """
    Compare sentiment across groups with statistical summary.

    Args:
        df: DataFrame with sentiment scores
        group_col: Column to group by
        sentiment_col: Sentiment score column

    Returns:
        Summary DataFrame
    """
    summary = df.groupby(group_col)[sentiment_col].agg([
        'count', 'mean', 'std', 'min', 'max',
        lambda x: x.quantile(0.25),
        lambda x: x.quantile(0.75)
    ]).round(3)

    summary.columns = ['count', 'mean', 'std', 'min', 'max', 'q25', 'q75']

    return summary


def analyze_extreme_sentiments(df: pd.DataFrame,
                               n_samples: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract most positive and most negative reviews.

    Args:
        df: DataFrame with sentiment scores
        n_samples: Number of samples to extract

    Returns:
        Tuple of (most_positive, most_negative) DataFrames
    """
    # Sort by sentiment
    sorted_df = df.sort_values('vader_compound')

    # Get extremes
    most_negative = sorted_df.head(n_samples).copy()
    most_positive = sorted_df.tail(n_samples).copy()

    return most_positive, most_negative


# ============================================================================
# KEYWORD-SPECIFIC SENTIMENT
# ============================================================================

def analyze_sentiment_by_keywords(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze sentiment for reviews containing specific keyword categories.

    Args:
        df: DataFrame with sentiment scores and keyword flags

    Returns:
        Summary DataFrame
    """
    keyword_cols = [c for c in df.columns if c.startswith('has_')]

    if not keyword_cols:
        logger.warning("No keyword category columns found")
        return pd.DataFrame()

    results = []

    for col in keyword_cols:
        category = col.replace('has_', '')

        # Reviews with keyword
        with_keyword = df[df[col] == True]
        # Reviews without keyword
        without_keyword = df[df[col] == False]

        results.append({
            'category': category,
            'with_keyword_n': len(with_keyword),
            'with_keyword_mean': with_keyword['vader_compound'].mean(),
            'with_keyword_std': with_keyword['vader_compound'].std(),
            'without_keyword_n': len(without_keyword),
            'without_keyword_mean': without_keyword['vader_compound'].mean(),
            'without_keyword_std': without_keyword['vader_compound'].std(),
            'difference': with_keyword['vader_compound'].mean() - without_keyword['vader_compound'].mean()
        })

    return pd.DataFrame(results).round(3)


# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def export_sentiment_report(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Export comprehensive sentiment analysis report.

    Args:
        df: DataFrame with sentiment scores
        output_dir: Directory to save report
    """
    timestamp = datetime.now().strftime('%Y%m%d')

    # Overall statistics
    overall_stats = df['vader_compound'].describe()
    overall_stats.to_csv(os.path.join(output_dir, f'sentiment_overall_stats_{timestamp}.csv'))

    # By app
    if 'app_name' in df.columns:
        app_stats = compare_sentiment_groups(df, 'app_name')
        app_stats.to_csv(os.path.join(output_dir, f'sentiment_by_app_{timestamp}.csv'))

    # By age category
    if 'age_category' in df.columns:
        age_stats = compare_sentiment_groups(df, 'age_category')
        age_stats.to_csv(os.path.join(output_dir, f'sentiment_by_age_{timestamp}.csv'))

    # By keyword
    keyword_stats = analyze_sentiment_by_keywords(df)
    if not keyword_stats.empty:
        keyword_stats.to_csv(os.path.join(output_dir, f'sentiment_by_keywords_{timestamp}.csv'))

    # Extreme reviews
    most_positive, most_negative = analyze_extreme_sentiments(df, 20)

    cols_to_save = ['review_text', 'vader_compound', 'app_name', 'age_category', 'sentiment_category']
    cols_to_save = [c for c in cols_to_save if c in df.columns]

    most_positive[cols_to_save].to_csv(
        os.path.join(output_dir, f'most_positive_reviews_{timestamp}.csv'),
        index=False
    )
    most_negative[cols_to_save].to_csv(
        os.path.join(output_dir, f'most_negative_reviews_{timestamp}.csv'),
        index=False
    )

    logger.info(f"Exported sentiment reports to {output_dir}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser(
        description="Run sentiment analysis for 'The For You Page Problem' research"
    )
    parser.add_argument('--input', type=str,
                       help='Path to input CSV file (or use latest in data/processed)')
    parser.add_argument('--export-report', action='store_true',
                       help='Export detailed sentiment report')

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = args.input
    else:
        # Find most recent cleaned file
        processed_files = glob.glob(os.path.join(INPUT_DIR, 'cleaned_*.csv'))
        if not processed_files:
            logger.error(f"No cleaned CSV files found in {INPUT_DIR}")
            exit(1)
        input_path = max(processed_files, key=os.path.getctime)
        logger.info(f"Using most recent file: {input_path}")

    # Run sentiment analysis
    df = run_sentiment_analysis(input_path)

    # Export report if requested
    if args.export_report:
        export_sentiment_report(df)

    logger.info("Done!")
