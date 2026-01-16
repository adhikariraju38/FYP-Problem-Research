"""
The 'For You' Page Problem: Data Visualization
==============================================
Creates publication-ready figures and charts.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import logging
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

try:
    from wordcloud import WordCloud
except ImportError:
    print("wordcloud not installed. Run: pip install wordcloud")

try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("plotly not installed. Run: pip install plotly")

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
OUTPUT_DIR = '../outputs/figures'

# Publication-quality settings
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams.update({
    'font.size': 12,
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight'
})

# Color palettes
PLATFORM_COLORS = {
    'tiktok': '#00f2ea',
    'instagram': '#E1306C',
    'youtube': '#FF0000',
    'facebook': '#1877F2'
}

SENTIMENT_COLORS = {
    'positive': '#2ecc71',
    'neutral': '#95a5a6',
    'negative': '#e74c3c'
}

AGE_COLORS = {
    'teen': '#3498db',
    'young_adult': '#9b59b6',
    'adult': '#f39c12',
    'parent': '#e74c3c',
    'older_adult': '#1abc9c',
    'unknown': '#bdc3c7'
}

# ============================================================================
# BASIC DISTRIBUTION PLOTS
# ============================================================================

def plot_platform_distribution(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Plot distribution of reviews by platform.

    Args:
        df: DataFrame with 'app_name' column
        output_dir: Directory to save figure
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    counts = df['app_name'].value_counts()
    colors = [PLATFORM_COLORS.get(app, '#666666') for app in counts.index]

    bars = ax.bar(counts.index, counts.values, color=colors, edgecolor='black', linewidth=0.5)

    ax.set_xlabel('Platform')
    ax.set_ylabel('Number of Reviews')
    ax.set_title('Distribution of Reviews by Platform')

    # Add value labels
    for bar, count in zip(bars, counts.values):
        ax.annotate(f'{count:,}',
                   xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                   ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'platform_distribution.png'))
    plt.savefig(os.path.join(output_dir, 'platform_distribution.pdf'))
    plt.close()
    logger.info("Saved platform distribution plot")


def plot_sentiment_distribution(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Plot overall sentiment distribution.

    Args:
        df: DataFrame with 'sentiment_category' column
        output_dir: Directory to save figure
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Pie chart
    counts = df['sentiment_category'].value_counts()
    colors = [SENTIMENT_COLORS.get(cat, '#666666') for cat in counts.index]

    axes[0].pie(counts.values, labels=counts.index, autopct='%1.1f%%',
                colors=colors, startangle=90, explode=[0.02]*len(counts))
    axes[0].set_title('Overall Sentiment Distribution')

    # Histogram of compound scores
    axes[1].hist(df['vader_compound'], bins=50, color='#3498db', edgecolor='black',
                 alpha=0.7, linewidth=0.5)
    axes[1].axvline(x=0, color='red', linestyle='--', label='Neutral')
    axes[1].axvline(x=df['vader_compound'].mean(), color='green', linestyle='-',
                   label=f'Mean ({df["vader_compound"].mean():.2f})')
    axes[1].set_xlabel('VADER Compound Score')
    axes[1].set_ylabel('Frequency')
    axes[1].set_title('Distribution of Sentiment Scores')
    axes[1].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sentiment_distribution.png'))
    plt.savefig(os.path.join(output_dir, 'sentiment_distribution.pdf'))
    plt.close()
    logger.info("Saved sentiment distribution plot")


def plot_age_distribution(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Plot distribution of inferred age categories.

    Args:
        df: DataFrame with 'age_category' column
        output_dir: Directory to save figure
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    counts = df['age_category'].value_counts()
    colors = [AGE_COLORS.get(cat, '#666666') for cat in counts.index]

    bars = ax.barh(counts.index, counts.values, color=colors, edgecolor='black', linewidth=0.5)

    ax.set_xlabel('Number of Reviews')
    ax.set_ylabel('Age Category')
    ax.set_title('Distribution of Inferred Age Categories')

    # Add value labels
    for bar, count in zip(bars, counts.values):
        ax.annotate(f'{count:,} ({count/len(df)*100:.1f}%)',
                   xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                   ha='left', va='center', fontsize=10, xytext=(5, 0),
                   textcoords='offset points')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'age_distribution.png'))
    plt.savefig(os.path.join(output_dir, 'age_distribution.pdf'))
    plt.close()
    logger.info("Saved age distribution plot")


# ============================================================================
# COMPARATIVE PLOTS
# ============================================================================

def plot_sentiment_by_platform(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Compare sentiment across platforms.

    Args:
        df: DataFrame with sentiment and app columns
        output_dir: Directory to save figure
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Box plot
    platforms = df['app_name'].unique()
    data = [df[df['app_name'] == p]['vader_compound'].values for p in platforms]
    colors = [PLATFORM_COLORS.get(p, '#666666') for p in platforms]

    bp = axes[0].boxplot(data, labels=platforms, patch_artist=True)
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    axes[0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
    axes[0].set_xlabel('Platform')
    axes[0].set_ylabel('VADER Compound Score')
    axes[0].set_title('Sentiment Distribution by Platform')

    # Stacked bar chart of sentiment categories
    sentiment_by_platform = pd.crosstab(df['app_name'], df['sentiment_category'], normalize='index') * 100

    sentiment_by_platform.plot(kind='bar', stacked=True, ax=axes[1],
                               color=[SENTIMENT_COLORS.get(c, '#666666')
                                     for c in sentiment_by_platform.columns])
    axes[1].set_xlabel('Platform')
    axes[1].set_ylabel('Percentage')
    axes[1].set_title('Sentiment Category Distribution by Platform')
    axes[1].legend(title='Sentiment', bbox_to_anchor=(1.02, 1))
    axes[1].tick_params(axis='x', rotation=0)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sentiment_by_platform.png'))
    plt.savefig(os.path.join(output_dir, 'sentiment_by_platform.pdf'))
    plt.close()
    logger.info("Saved sentiment by platform plot")


def plot_sentiment_by_age(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Compare sentiment across age categories.

    Args:
        df: DataFrame with sentiment and age columns
        output_dir: Directory to save figure
    """
    # Filter out unknown age
    df_known_age = df[df['age_category'] != 'unknown']

    if len(df_known_age) < 100:
        logger.warning("Not enough data with known age categories for visualization")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Box plot
    age_order = ['teen', 'young_adult', 'adult', 'parent', 'older_adult']
    age_order = [a for a in age_order if a in df_known_age['age_category'].values]

    data = [df_known_age[df_known_age['age_category'] == a]['vader_compound'].values
            for a in age_order]
    colors = [AGE_COLORS.get(a, '#666666') for a in age_order]

    bp = axes[0].boxplot(data, labels=age_order, patch_artist=True)
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    axes[0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
    axes[0].set_xlabel('Age Category')
    axes[0].set_ylabel('VADER Compound Score')
    axes[0].set_title('Sentiment Distribution by Age Category')
    axes[0].tick_params(axis='x', rotation=45)

    # Bar chart of mean sentiment
    mean_sentiment = df_known_age.groupby('age_category')['vader_compound'].mean()
    mean_sentiment = mean_sentiment.reindex(age_order)
    colors = [AGE_COLORS.get(a, '#666666') for a in mean_sentiment.index]

    bars = axes[1].bar(mean_sentiment.index, mean_sentiment.values, color=colors,
                       edgecolor='black', linewidth=0.5)
    axes[1].axhline(y=0, color='red', linestyle='--', alpha=0.5)
    axes[1].set_xlabel('Age Category')
    axes[1].set_ylabel('Mean Sentiment Score')
    axes[1].set_title('Mean Sentiment by Age Category')
    axes[1].tick_params(axis='x', rotation=45)

    # Add error bars
    std_sentiment = df_known_age.groupby('age_category')['vader_compound'].std()
    std_sentiment = std_sentiment.reindex(age_order)
    axes[1].errorbar(range(len(mean_sentiment)), mean_sentiment.values,
                    yerr=std_sentiment.values, fmt='none', color='black', capsize=5)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'sentiment_by_age.png'))
    plt.savefig(os.path.join(output_dir, 'sentiment_by_age.pdf'))
    plt.close()
    logger.info("Saved sentiment by age plot")


def plot_keyword_category_analysis(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Plot analysis of keyword categories.

    Args:
        df: DataFrame with keyword flag columns
        output_dir: Directory to save figure
    """
    keyword_cols = [c for c in df.columns if c.startswith('has_')]

    if not keyword_cols:
        logger.warning("No keyword columns found")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Count of reviews per category
    counts = {col.replace('has_', ''): df[col].sum() for col in keyword_cols}
    categories = list(counts.keys())
    values = list(counts.values())

    bars = axes[0].barh(categories, values, color='#3498db', edgecolor='black', linewidth=0.5)
    axes[0].set_xlabel('Number of Reviews')
    axes[0].set_ylabel('Keyword Category')
    axes[0].set_title('Reviews Mentioning Each Category')

    # Add percentage labels
    total = len(df)
    for bar, val in zip(bars, values):
        axes[0].annotate(f'{val:,} ({val/total*100:.1f}%)',
                        xy=(bar.get_width(), bar.get_y() + bar.get_height()/2),
                        ha='left', va='center', fontsize=9, xytext=(5, 0),
                        textcoords='offset points')

    # Sentiment by keyword category
    sentiment_data = []
    for col in keyword_cols:
        category = col.replace('has_', '')
        with_kw = df[df[col] == True]['vader_compound'].mean()
        without_kw = df[df[col] == False]['vader_compound'].mean()
        sentiment_data.append({
            'category': category,
            'with_keyword': with_kw,
            'without_keyword': without_kw
        })

    sentiment_df = pd.DataFrame(sentiment_data)

    x = np.arange(len(sentiment_df))
    width = 0.35

    bars1 = axes[1].bar(x - width/2, sentiment_df['with_keyword'], width,
                        label='With Keyword', color='#e74c3c', edgecolor='black', linewidth=0.5)
    bars2 = axes[1].bar(x + width/2, sentiment_df['without_keyword'], width,
                        label='Without Keyword', color='#2ecc71', edgecolor='black', linewidth=0.5)

    axes[1].axhline(y=0, color='black', linestyle='-', alpha=0.3)
    axes[1].set_xlabel('Keyword Category')
    axes[1].set_ylabel('Mean Sentiment Score')
    axes[1].set_title('Mean Sentiment: With vs Without Keywords')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(sentiment_df['category'], rotation=45, ha='right')
    axes[1].legend()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'keyword_category_analysis.png'))
    plt.savefig(os.path.join(output_dir, 'keyword_category_analysis.pdf'))
    plt.close()
    logger.info("Saved keyword category analysis plot")


# ============================================================================
# WORD CLOUDS
# ============================================================================

def create_wordcloud(df: pd.DataFrame,
                    text_col: str = 'review_text_clean',
                    title: str = 'Word Cloud',
                    output_path: str = None,
                    mask: np.ndarray = None,
                    colormap: str = 'viridis'):
    """
    Create a word cloud from text data.

    Args:
        df: DataFrame with text column
        text_col: Name of text column
        title: Title for the plot
        output_path: Path to save figure
        mask: Optional mask array
        colormap: Matplotlib colormap name
    """
    # Combine all text
    text = ' '.join(df[text_col].dropna().astype(str))

    if not text.strip():
        logger.warning("No text data for word cloud")
        return

    # Create word cloud
    wc = WordCloud(
        width=1600,
        height=800,
        background_color='white',
        max_words=200,
        colormap=colormap,
        mask=mask,
        contour_width=1,
        contour_color='steelblue'
    ).generate(text)

    # Plot
    try:
        fig, ax = plt.subplots(figsize=(16, 8))
        ax.imshow(wc.to_array(), interpolation='bilinear')
        ax.axis('off')
        ax.set_title(title, fontsize=16, fontweight='bold')

        plt.tight_layout()
        if output_path:
            plt.savefig(output_path)
            plt.savefig(output_path.replace('.png', '.pdf'))
        plt.close()
    except Exception as e:
        logger.warning(f"Error creating word cloud plot: {str(e)}")
        # Try saving wordcloud directly without matplotlib
        if output_path:
            wc.to_file(output_path)
            logger.info(f"Saved word cloud directly to {output_path}")
        plt.close('all')


def create_wordclouds_by_category(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Create word clouds for different categories.

    Args:
        df: DataFrame with text and category columns
        output_dir: Directory to save figures
    """
    text_col = 'review_text_clean' if 'review_text_clean' in df.columns else 'review_text'

    # Overall word cloud
    create_wordcloud(df, text_col, 'All Reviews - Word Cloud',
                    os.path.join(output_dir, 'wordcloud_all.png'))

    # By platform
    for platform in df['app_name'].unique():
        platform_df = df[df['app_name'] == platform]
        create_wordcloud(platform_df, text_col, f'{platform.title()} Reviews - Word Cloud',
                        os.path.join(output_dir, f'wordcloud_{platform}.png'))

    # By sentiment
    for sentiment in ['positive', 'negative']:
        if 'sentiment_category' in df.columns:
            sent_df = df[df['sentiment_category'] == sentiment]
            create_wordcloud(sent_df, text_col, f'{sentiment.title()} Reviews - Word Cloud',
                            os.path.join(output_dir, f'wordcloud_{sentiment}.png'),
                            colormap='Greens' if sentiment == 'positive' else 'Reds')

    # By keyword category
    keyword_cols = [c for c in df.columns if c.startswith('has_')]
    for col in keyword_cols:
        category = col.replace('has_', '')
        cat_df = df[df[col] == True]
        if len(cat_df) > 50:
            create_wordcloud(cat_df, text_col, f'{category.replace("_", " ").title()} Reviews',
                            os.path.join(output_dir, f'wordcloud_{category}.png'))

    logger.info("Created word clouds")


# ============================================================================
# HEATMAPS
# ============================================================================

def create_correlation_heatmap(df: pd.DataFrame, output_dir: str = OUTPUT_DIR):
    """
    Create correlation heatmap for numerical variables.

    Args:
        df: DataFrame
        output_dir: Directory to save figure
    """
    # Select numerical columns
    num_cols = ['vader_compound', 'textblob_polarity', 'textblob_subjectivity',
                'rating', 'word_count', 'helpful_count']
    num_cols = [c for c in num_cols if c in df.columns]

    # Add keyword flags
    keyword_cols = [c for c in df.columns if c.startswith('has_')]
    df_temp = df.copy()
    for col in keyword_cols:
        df_temp[col] = df_temp[col].astype(int)
    num_cols.extend(keyword_cols)

    if len(num_cols) < 3:
        logger.warning("Not enough numerical columns for heatmap")
        return

    # Calculate correlation
    corr = df_temp[num_cols].corr()

    # Plot
    fig, ax = plt.subplots(figsize=(12, 10))
    mask = np.triu(np.ones_like(corr, dtype=bool))

    sns.heatmap(corr, mask=mask, cmap='RdBu_r', center=0,
                annot=True, fmt='.2f', square=True, linewidths=0.5,
                ax=ax, vmin=-1, vmax=1,
                cbar_kws={'shrink': 0.8, 'label': 'Correlation'})

    ax.set_title('Correlation Matrix', fontsize=14, fontweight='bold')

    # Rotate labels
    ax.set_xticklabels([c.replace('has_', '').replace('_', ' ').title()
                       for c in corr.columns], rotation=45, ha='right')
    ax.set_yticklabels([c.replace('has_', '').replace('_', ' ').title()
                       for c in corr.index], rotation=0)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'correlation_heatmap.png'))
    plt.savefig(os.path.join(output_dir, 'correlation_heatmap.pdf'))
    plt.close()
    logger.info("Saved correlation heatmap")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def create_all_visualizations(input_path: str, output_dir: str = OUTPUT_DIR):
    """
    Create all visualizations.

    Args:
        input_path: Path to analyzed CSV file
        output_dir: Directory to save figures
    """
    logger.info(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path, encoding='utf-8')
    logger.info(f"Loaded {len(df)} reviews")

    os.makedirs(output_dir, exist_ok=True)

    # Create all plots
    logger.info("Creating visualizations...")

    if 'app_name' in df.columns:
        plot_platform_distribution(df, output_dir)

    if 'sentiment_category' in df.columns:
        plot_sentiment_distribution(df, output_dir)

    if 'age_category' in df.columns:
        plot_age_distribution(df, output_dir)

    if 'app_name' in df.columns and 'vader_compound' in df.columns:
        plot_sentiment_by_platform(df, output_dir)

    if 'age_category' in df.columns and 'vader_compound' in df.columns:
        plot_sentiment_by_age(df, output_dir)

    plot_keyword_category_analysis(df, output_dir)
    create_wordclouds_by_category(df, output_dir)
    create_correlation_heatmap(df, output_dir)

    logger.info(f"All visualizations saved to: {output_dir}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser(
        description="Create visualizations for 'The For You Page Problem' research"
    )
    parser.add_argument('--input', type=str,
                       help='Path to input CSV file')

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = args.input
    else:
        # Find most recent analyzed file
        processed_files = glob.glob(os.path.join(INPUT_DIR, 'sentiment_analyzed_*.csv'))
        if not processed_files:
            processed_files = glob.glob(os.path.join(INPUT_DIR, 'cleaned_*.csv'))
        if not processed_files:
            logger.error(f"No processed CSV files found in {INPUT_DIR}")
            exit(1)
        input_path = max(processed_files, key=os.path.getctime)
        logger.info(f"Using most recent file: {input_path}")

    create_all_visualizations(input_path)
    logger.info("Done!")
