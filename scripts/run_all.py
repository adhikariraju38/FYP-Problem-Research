"""
The 'For You' Page Problem: Main Pipeline Runner
================================================
Runs the complete analysis pipeline from data collection to visualization.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'pipeline_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(skip_scraping: bool = False,
                 skip_cleaning: bool = False,
                 skip_sentiment: bool = False,
                 skip_topics: bool = False,
                 skip_visualization: bool = False,
                 test_mode: bool = False):
    """
    Run the complete analysis pipeline.

    Args:
        skip_scraping: Skip data scraping step
        skip_cleaning: Skip data cleaning step
        skip_sentiment: Skip sentiment analysis step
        skip_topics: Skip topic modeling step
        skip_visualization: Skip visualization step
        test_mode: Run in test mode (smaller sample)
    """
    import importlib.util
    import glob

    logger.info("=" * 60)
    logger.info("THE 'FOR YOU' PAGE PROBLEM - ANALYSIS PIPELINE")
    logger.info("=" * 60)
    logger.info(f"Started at: {datetime.now().isoformat()}")

    # Step 1: Data Scraping
    if not skip_scraping:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 1: DATA SCRAPING")
        logger.info("=" * 60)

        # Import with correct module names (01_scrape_reviews.py -> module name)
        spec = importlib.util.spec_from_file_location("scrape_reviews", "01_scrape_reviews.py")
        scrape_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scrape_module)
        scrape_all_platforms = scrape_module.scrape_all_platforms
        quick_test = scrape_module.quick_test

        if test_mode:
            logger.info("Running in TEST MODE - scraping 100 reviews only")
            quick_test('tiktok', 100)
        else:
            scrape_all_platforms(
                max_reviews_per_app=10000,
                countries=['np', 'in']
            )
    else:
        logger.info("Skipping data scraping...")

    # Step 2: Data Cleaning
    if not skip_cleaning:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: DATA CLEANING")
        logger.info("=" * 60)

        spec = importlib.util.spec_from_file_location("clean_data", "02_clean_data.py")
        clean_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(clean_module)
        clean_dataset = clean_module.clean_dataset
        create_analysis_subsets = clean_module.create_analysis_subsets

        # Find most recent raw data file
        raw_files = glob.glob('../data/raw/*.csv')
        if not raw_files:
            logger.error("No raw data files found. Run scraping first.")
            return

        input_path = max(raw_files, key=os.path.getctime)
        df = clean_dataset(input_path)
        create_analysis_subsets(df)
    else:
        logger.info("Skipping data cleaning...")

    # Step 3: Sentiment Analysis
    if not skip_sentiment:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 3: SENTIMENT ANALYSIS")
        logger.info("=" * 60)

        spec = importlib.util.spec_from_file_location("sentiment_analysis", "03_sentiment_analysis.py")
        sentiment_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sentiment_module)
        run_sentiment_analysis = sentiment_module.run_sentiment_analysis
        export_sentiment_report = sentiment_module.export_sentiment_report

        # Find most recent cleaned file
        processed_files = glob.glob('../data/processed/cleaned_*.csv')
        if not processed_files:
            logger.error("No cleaned data files found. Run cleaning first.")
            return

        input_path = max(processed_files, key=os.path.getctime)
        df = run_sentiment_analysis(input_path)
        export_sentiment_report(df)
    else:
        logger.info("Skipping sentiment analysis...")

    # Step 4: Topic Modeling
    if not skip_topics:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 4: TOPIC MODELING")
        logger.info("=" * 60)

        spec = importlib.util.spec_from_file_location("topic_modeling", "04_topic_modeling.py")
        topic_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(topic_module)
        run_topic_modeling = topic_module.run_topic_modeling

        # Find most recent analyzed file
        processed_files = glob.glob('../data/processed/sentiment_analyzed_*.csv')
        if not processed_files:
            processed_files = glob.glob('../data/processed/cleaned_*.csv')
        if not processed_files:
            logger.error("No processed files found.")
            return

        input_path = max(processed_files, key=os.path.getctime)
        run_topic_modeling(input_path, find_optimal=not test_mode)
    else:
        logger.info("Skipping topic modeling...")

    # Step 5: Visualization
    if not skip_visualization:
        logger.info("\n" + "=" * 60)
        logger.info("STEP 5: VISUALIZATION")
        logger.info("=" * 60)

        spec = importlib.util.spec_from_file_location("visualizations", "05_visualizations.py")
        vis_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(vis_module)
        create_all_visualizations = vis_module.create_all_visualizations

        # Find most recent analyzed file
        processed_files = glob.glob('../data/processed/sentiment_analyzed_*.csv')
        if not processed_files:
            processed_files = glob.glob('../data/processed/cleaned_*.csv')
        if not processed_files:
            logger.error("No processed files found.")
            return

        input_path = max(processed_files, key=os.path.getctime)
        create_all_visualizations(input_path)
    else:
        logger.info("Skipping visualization...")

    # Done
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Finished at: {datetime.now().isoformat()}")
    logger.info("\nOutput locations:")
    logger.info("  Raw data:       ../data/raw/")
    logger.info("  Processed data: ../data/processed/")
    logger.info("  Topics:         ../outputs/topics/")
    logger.info("  Figures:        ../outputs/figures/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run 'The For You Page Problem' analysis pipeline"
    )
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip data scraping step')
    parser.add_argument('--skip-cleaning', action='store_true',
                       help='Skip data cleaning step')
    parser.add_argument('--skip-sentiment', action='store_true',
                       help='Skip sentiment analysis step')
    parser.add_argument('--skip-topics', action='store_true',
                       help='Skip topic modeling step')
    parser.add_argument('--skip-visualization', action='store_true',
                       help='Skip visualization step')
    parser.add_argument('--test', action='store_true',
                       help='Run in test mode (smaller sample)')

    args = parser.parse_args()

    # Change to scripts directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    run_pipeline(
        skip_scraping=args.skip_scraping,
        skip_cleaning=args.skip_cleaning,
        skip_sentiment=args.skip_sentiment,
        skip_topics=args.skip_topics,
        skip_visualization=args.skip_visualization,
        test_mode=args.test
    )
