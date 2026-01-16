"""
The 'For You' Page Problem: Social Media Scraping
==================================================
Scrapes posts and comments from Twitter, Reddit, and TikTok.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups

NOTE: Facebook and Instagram have very strict API policies.
      For those platforms, manual collection is recommended.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('social_media_scraping_log.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

OUTPUT_DIR = '../data/raw'

# Search keywords for finding relevant posts
SEARCH_KEYWORDS = [
    # Explicit content related
    'tiktok inappropriate content',
    'reels explicit',
    'tiktok 18+',
    'instagram inappropriate',
    'youtube shorts explicit',
    'tiktok nudity',
    'reels vulgar',

    # Age/children related
    'kids on tiktok',
    'children watching reels',
    'teenager tiktok addiction',
    'my child tiktok',
    'parental control tiktok',

    # Mental health related
    'tiktok addiction',
    'reels mental health',
    'tiktok depression',
    'social media addiction teens',

    # Nepal specific
    'tiktok nepal',
    'reels nepal',
    'nepali tiktok',
    '#TikTokNepal',
    '#ReelsNepal',

    # Algorithm complaints
    'fyp inappropriate',
    'for you page problem',
    'tiktok algorithm showing',
]

# Subreddits to scrape
SUBREDDITS = [
    'TikTok',
    'Instagram',
    'youtube',
    'socialmedia',
    'Nepal',
    'Parenting',
    'teenagers',
    'internetparents',
    'NoSurfing',
    'digitalminimalism'
]

# ============================================================================
# REDDIT SCRAPING (Using PRAW)
# ============================================================================

def setup_reddit():
    """
    Set up Reddit API connection.

    You need to create a Reddit app at: https://www.reddit.com/prefs/apps
    Then set environment variables or replace with your credentials.
    """
    try:
        import praw
    except ImportError:
        logger.error("PRAW not installed. Run: pip install praw")
        return None

    # Check for credentials
    client_id = os.environ.get('REDDIT_CLIENT_ID', 'YOUR_CLIENT_ID')
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET', 'YOUR_CLIENT_SECRET')
    user_agent = 'FYP-Problem-Research/1.0'

    if client_id == 'YOUR_CLIENT_ID':
        logger.warning("""
        ========================================
        REDDIT API SETUP REQUIRED
        ========================================
        1. Go to https://www.reddit.com/prefs/apps
        2. Create a new app (script type)
        3. Set environment variables:
           export REDDIT_CLIENT_ID='your_id'
           export REDDIT_CLIENT_SECRET='your_secret'

        Or edit this script and replace the credentials directly.
        ========================================
        """)
        return None

    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        # Test connection
        reddit.user.me()
        logger.info("Reddit API connected successfully")
        return reddit
    except Exception as e:
        logger.error(f"Reddit API connection failed: {str(e)}")
        return None


def scrape_reddit_posts(reddit, subreddits: List[str] = SUBREDDITS,
                        keywords: List[str] = SEARCH_KEYWORDS,
                        limit_per_search: int = 100) -> pd.DataFrame:
    """
    Scrape Reddit posts and comments related to research keywords.
    """
    if reddit is None:
        return pd.DataFrame()

    all_posts = []

    # Search by keywords across Reddit
    logger.info("Searching Reddit by keywords...")
    for keyword in tqdm(keywords[:10], desc="Searching keywords"):  # Limit to avoid rate limits
        try:
            for submission in reddit.subreddit('all').search(keyword, limit=limit_per_search):
                post_data = {
                    'post_id': submission.id,
                    'title': submission.title,
                    'text': submission.selftext,
                    'author': str(submission.author) if submission.author else '[deleted]',
                    'subreddit': str(submission.subreddit),
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'created_utc': datetime.fromtimestamp(submission.created_utc),
                    'url': submission.url,
                    'search_keyword': keyword,
                    'platform': 'reddit',
                    'content_type': 'post'
                }
                all_posts.append(post_data)
            time.sleep(1)  # Rate limiting
        except Exception as e:
            logger.warning(f"Error searching for '{keyword}': {str(e)}")
            continue

    # Scrape specific subreddits
    logger.info("Scraping specific subreddits...")
    for subreddit_name in tqdm(subreddits, desc="Scraping subreddits"):
        try:
            subreddit = reddit.subreddit(subreddit_name)
            for submission in subreddit.hot(limit=50):
                post_data = {
                    'post_id': submission.id,
                    'title': submission.title,
                    'text': submission.selftext,
                    'author': str(submission.author) if submission.author else '[deleted]',
                    'subreddit': subreddit_name,
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'created_utc': datetime.fromtimestamp(submission.created_utc),
                    'url': submission.url,
                    'search_keyword': f'subreddit:{subreddit_name}',
                    'platform': 'reddit',
                    'content_type': 'post'
                }
                all_posts.append(post_data)
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Error scraping r/{subreddit_name}: {str(e)}")
            continue

    df = pd.DataFrame(all_posts)
    df = df.drop_duplicates(subset=['post_id'])

    # Combine title and text for analysis
    df['full_text'] = df['title'] + ' ' + df['text'].fillna('')

    logger.info(f"Scraped {len(df)} Reddit posts")
    return df


def scrape_reddit_comments(reddit, post_ids: List[str],
                          max_comments_per_post: int = 50) -> pd.DataFrame:
    """
    Scrape comments from specific Reddit posts.
    """
    if reddit is None or not post_ids:
        return pd.DataFrame()

    all_comments = []

    logger.info(f"Scraping comments from {len(post_ids)} posts...")
    for post_id in tqdm(post_ids[:100], desc="Scraping comments"):  # Limit to avoid rate limits
        try:
            submission = reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)  # Skip "load more" links

            for comment in submission.comments.list()[:max_comments_per_post]:
                comment_data = {
                    'comment_id': comment.id,
                    'post_id': post_id,
                    'text': comment.body,
                    'author': str(comment.author) if comment.author else '[deleted]',
                    'score': comment.score,
                    'created_utc': datetime.fromtimestamp(comment.created_utc),
                    'platform': 'reddit',
                    'content_type': 'comment'
                }
                all_comments.append(comment_data)
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Error getting comments for post {post_id}: {str(e)}")
            continue

    df = pd.DataFrame(all_comments)
    logger.info(f"Scraped {len(df)} Reddit comments")
    return df


# ============================================================================
# TWITTER/X SCRAPING (Using Tweepy)
# ============================================================================

def setup_twitter():
    """
    Set up Twitter API connection.

    You need Twitter API credentials (free tier available at developer.twitter.com)
    """
    try:
        import tweepy
    except ImportError:
        logger.error("Tweepy not installed. Run: pip install tweepy")
        return None

    bearer_token = os.environ.get('TWITTER_BEARER_TOKEN', 'YOUR_BEARER_TOKEN')

    if bearer_token == 'YOUR_BEARER_TOKEN':
        logger.warning("""
        ========================================
        TWITTER API SETUP REQUIRED
        ========================================
        1. Go to https://developer.twitter.com
        2. Create a project and get Bearer Token
        3. Set environment variable:
           export TWITTER_BEARER_TOKEN='your_token'

        Note: Free tier has limited access (100 tweets/month for search)
        ========================================
        """)
        return None

    try:
        client = tweepy.Client(bearer_token=bearer_token)
        logger.info("Twitter API connected successfully")
        return client
    except Exception as e:
        logger.error(f"Twitter API connection failed: {str(e)}")
        return None


def scrape_twitter(client, keywords: List[str] = SEARCH_KEYWORDS[:5],
                  max_results_per_query: int = 100) -> pd.DataFrame:
    """
    Scrape tweets related to research keywords.

    Note: Free tier is very limited (100 tweets/month for recent search)
    """
    if client is None:
        return pd.DataFrame()

    all_tweets = []

    logger.info("Searching Twitter...")
    for keyword in tqdm(keywords, desc="Searching Twitter"):
        try:
            # Build query - exclude retweets, include English
            query = f'{keyword} -is:retweet lang:en'

            tweets = client.search_recent_tweets(
                query=query,
                max_results=min(max_results_per_query, 100),
                tweet_fields=['created_at', 'public_metrics', 'author_id', 'text']
            )

            if tweets.data:
                for tweet in tweets.data:
                    tweet_data = {
                        'tweet_id': tweet.id,
                        'text': tweet.text,
                        'author_id': tweet.author_id,
                        'created_at': tweet.created_at,
                        'retweet_count': tweet.public_metrics.get('retweet_count', 0),
                        'like_count': tweet.public_metrics.get('like_count', 0),
                        'reply_count': tweet.public_metrics.get('reply_count', 0),
                        'search_keyword': keyword,
                        'platform': 'twitter',
                        'content_type': 'tweet'
                    }
                    all_tweets.append(tweet_data)

            time.sleep(1)  # Rate limiting

        except Exception as e:
            logger.warning(f"Error searching Twitter for '{keyword}': {str(e)}")
            continue

    df = pd.DataFrame(all_tweets)
    if not df.empty:
        df = df.drop_duplicates(subset=['tweet_id'])

    logger.info(f"Scraped {len(df)} tweets")
    return df


# ============================================================================
# YOUTUBE COMMENTS SCRAPING
# ============================================================================

def scrape_youtube_comments(video_ids: List[str] = None,
                           search_queries: List[str] = None) -> pd.DataFrame:
    """
    Scrape YouTube comments using youtube-comment-downloader.

    Provide either video_ids or search_queries.
    """
    try:
        from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR
    except ImportError:
        logger.error("youtube-comment-downloader not installed. Run: pip install youtube-comment-downloader")
        return pd.DataFrame()

    downloader = YoutubeCommentDownloader()
    all_comments = []

    # Default video IDs about TikTok/reels issues (you can add more)
    if video_ids is None:
        video_ids = []
        logger.info("No video IDs provided. Skipping YouTube comments.")
        logger.info("To scrape YouTube, provide video IDs of relevant videos about TikTok/Reels issues.")
        return pd.DataFrame()

    logger.info(f"Scraping comments from {len(video_ids)} YouTube videos...")
    for video_id in tqdm(video_ids, desc="Scraping YouTube"):
        try:
            comments = downloader.get_comments_from_url(
                f'https://www.youtube.com/watch?v={video_id}',
                sort_by=SORT_BY_POPULAR
            )

            count = 0
            for comment in comments:
                if count >= 200:  # Limit per video
                    break

                comment_data = {
                    'comment_id': comment.get('cid'),
                    'video_id': video_id,
                    'text': comment.get('text'),
                    'author': comment.get('author'),
                    'likes': comment.get('votes', 0),
                    'time': comment.get('time'),
                    'platform': 'youtube',
                    'content_type': 'comment'
                }
                all_comments.append(comment_data)
                count += 1

            time.sleep(2)
        except Exception as e:
            logger.warning(f"Error scraping video {video_id}: {str(e)}")
            continue

    df = pd.DataFrame(all_comments)
    logger.info(f"Scraped {len(df)} YouTube comments")
    return df


# ============================================================================
# MANUAL DATA COLLECTION TEMPLATE
# ============================================================================

def create_manual_collection_template(output_dir: str = OUTPUT_DIR):
    """
    Create Excel templates for manual data collection from Facebook and Instagram.

    Since these platforms restrict automated scraping, manual collection is safer.
    """

    # Template columns
    columns = [
        'post_id',
        'text',
        'author_username',
        'author_display_name',
        'post_date',
        'likes',
        'comments_count',
        'shares',
        'hashtags',
        'platform',
        'url',
        'notes',
        'is_about_explicit_content',
        'mentions_age_group',
        'mentions_mental_health'
    ]

    # Create empty templates
    facebook_template = pd.DataFrame(columns=columns)
    instagram_template = pd.DataFrame(columns=columns)
    tiktok_template = pd.DataFrame(columns=columns)

    # Add example rows
    facebook_template.loc[0] = {
        'post_id': 'EXAMPLE_123',
        'text': 'Copy the post text here...',
        'author_username': 'example_user',
        'author_display_name': 'Example User',
        'post_date': '2026-01-15',
        'likes': 100,
        'comments_count': 25,
        'shares': 10,
        'hashtags': '#TikTokNepal, #Reels',
        'platform': 'facebook',
        'url': 'https://facebook.com/...',
        'notes': 'Found in Nepal Tech group',
        'is_about_explicit_content': 'Yes',
        'mentions_age_group': 'teenagers',
        'mentions_mental_health': 'No'
    }

    os.makedirs(output_dir, exist_ok=True)

    # Save templates
    facebook_template.to_excel(
        os.path.join(output_dir, 'manual_collection_facebook.xlsx'),
        index=False
    )
    instagram_template.to_excel(
        os.path.join(output_dir, 'manual_collection_instagram.xlsx'),
        index=False
    )
    tiktok_template.to_excel(
        os.path.join(output_dir, 'manual_collection_tiktok.xlsx'),
        index=False
    )

    logger.info(f"""
    ========================================
    MANUAL COLLECTION TEMPLATES CREATED
    ========================================

    Templates saved to {output_dir}:
    - manual_collection_facebook.xlsx
    - manual_collection_instagram.xlsx
    - manual_collection_tiktok.xlsx

    HOW TO USE:

    1. FACEBOOK:
       - Join Nepal-based groups discussing social media
       - Search for posts about TikTok, Reels, explicit content
       - Copy relevant posts/comments to the template

    2. INSTAGRAM:
       - Search hashtags: #TikTokNepal, #ReelsNepal
       - Look at comments on viral reels about the topic
       - Copy relevant content to template

    3. TIKTOK:
       - Search for videos discussing explicit content issues
       - Copy comments from relevant videos
       - Note the video context

    SEARCH SUGGESTIONS:
    - "tiktok nepal inappropriate"
    - "reels showing explicit content"
    - "my child on tiktok"
    - "fyp problem nepal"
    - Comments on news articles about TikTok bans

    ========================================
    """)


# ============================================================================
# MAIN SCRAPING FUNCTION
# ============================================================================

def scrape_all_social_media(output_dir: str = OUTPUT_DIR,
                           scrape_reddit: bool = True,
                           scrape_twitter: bool = True,
                           scrape_youtube: bool = False,
                           create_templates: bool = True) -> Dict[str, pd.DataFrame]:
    """
    Scrape data from all available social media platforms.
    """
    os.makedirs(output_dir, exist_ok=True)
    results = {}
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Reddit
    if scrape_reddit:
        logger.info("\n" + "=" * 50)
        logger.info("SCRAPING REDDIT")
        logger.info("=" * 50)

        reddit = setup_reddit()
        if reddit:
            posts_df = scrape_reddit_posts(reddit)
            if not posts_df.empty:
                # Get comments from relevant posts
                relevant_posts = posts_df[posts_df['num_comments'] > 5]['post_id'].tolist()
                comments_df = scrape_reddit_comments(reddit, relevant_posts[:50])

                # Combine
                posts_df['content_type'] = 'post'
                if not comments_df.empty:
                    reddit_df = pd.concat([posts_df, comments_df], ignore_index=True)
                else:
                    reddit_df = posts_df

                reddit_df.to_csv(
                    os.path.join(output_dir, f'reddit_data_{timestamp}.csv'),
                    index=False, encoding='utf-8'
                )
                results['reddit'] = reddit_df
                logger.info(f"Saved {len(reddit_df)} Reddit items")

    # Twitter
    if scrape_twitter:
        logger.info("\n" + "=" * 50)
        logger.info("SCRAPING TWITTER")
        logger.info("=" * 50)

        twitter = setup_twitter()
        if twitter:
            twitter_df = scrape_twitter(twitter)
            if not twitter_df.empty:
                twitter_df.to_csv(
                    os.path.join(output_dir, f'twitter_data_{timestamp}.csv'),
                    index=False, encoding='utf-8'
                )
                results['twitter'] = twitter_df
                logger.info(f"Saved {len(twitter_df)} tweets")

    # YouTube
    if scrape_youtube:
        logger.info("\n" + "=" * 50)
        logger.info("SCRAPING YOUTUBE COMMENTS")
        logger.info("=" * 50)

        # Add relevant video IDs here
        video_ids = []  # User needs to provide
        youtube_df = scrape_youtube_comments(video_ids)
        if not youtube_df.empty:
            youtube_df.to_csv(
                os.path.join(output_dir, f'youtube_comments_{timestamp}.csv'),
                index=False, encoding='utf-8'
            )
            results['youtube'] = youtube_df

    # Create manual collection templates
    if create_templates:
        logger.info("\n" + "=" * 50)
        logger.info("CREATING MANUAL COLLECTION TEMPLATES")
        logger.info("=" * 50)
        create_manual_collection_template(output_dir)

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("SOCIAL MEDIA SCRAPING SUMMARY")
    logger.info("=" * 50)

    total = 0
    for platform, df in results.items():
        count = len(df)
        total += count
        logger.info(f"  {platform}: {count} items")

    logger.info(f"\nTotal social media items: {total}")
    logger.info(f"Data saved to: {output_dir}")

    return results


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape social media for 'The For You Page Problem' research"
    )
    parser.add_argument('--reddit', action='store_true', default=True,
                       help='Scrape Reddit (default: True)')
    parser.add_argument('--twitter', action='store_true', default=True,
                       help='Scrape Twitter (requires API key)')
    parser.add_argument('--youtube', action='store_true',
                       help='Scrape YouTube comments')
    parser.add_argument('--templates-only', action='store_true',
                       help='Only create manual collection templates')

    args = parser.parse_args()

    if args.templates_only:
        create_manual_collection_template()
    else:
        scrape_all_social_media(
            scrape_reddit=args.reddit,
            scrape_twitter=args.twitter,
            scrape_youtube=args.youtube
        )

    logger.info("Done!")
