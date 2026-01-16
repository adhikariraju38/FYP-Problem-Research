"""
The 'For You' Page Problem: Topic Modeling
==========================================
Performs LDA topic modeling on review data to discover themes.

Author: [Your Name]
Research: Impact of Explicit Content in Short-Form Videos on Different Age Groups
"""

import os
import logging
import warnings
from datetime import datetime
from typing import List, Tuple, Dict

import pandas as pd
import numpy as np
from tqdm import tqdm

# NLP imports
try:
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('wordnet', quiet=True)
    nltk.download('punkt_tab', quiet=True)
except ImportError:
    print("NLTK not installed. Run: pip install nltk")

# Topic modeling
try:
    import gensim
    from gensim import corpora
    from gensim.models import LdaMulticore, CoherenceModel
except ImportError:
    print("Gensim not installed. Run: pip install gensim")

# Visualization
try:
    import pyLDAvis
    import pyLDAvis.gensim_models
except ImportError:
    print("pyLDAvis not installed. Run: pip install pyLDAvis")

warnings.filterwarnings('ignore')

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
OUTPUT_DIR = '../outputs/topics'

# Custom stopwords for social media review context
CUSTOM_STOPWORDS = {
    'app', 'application', 'tiktok', 'instagram', 'youtube', 'facebook',
    'reel', 'reels', 'video', 'videos', 'short', 'shorts',
    'please', 'thank', 'thanks', 'like', 'would', 'could', 'really',
    'get', 'got', 'use', 'using', 'used', 'make', 'made', 'just',
    'one', 'also', 'even', 'much', 'many', 'lot', 'lots', 'thing',
    'good', 'bad', 'great', 'best', 'worst', 'love', 'hate',
    'star', 'stars', 'give', 'giving', 'review', 'update', 'version',
    'phone', 'device', 'android', 'iphone', 'ios', 'fix', 'fixed',
    'work', 'working', 'doesnt', "doesn't", 'dont', "don't", 'cant', "can't"
}

# Number of topics to try
NUM_TOPICS_RANGE = [5, 7, 10, 12, 15]

# ============================================================================
# TEXT PREPROCESSING FOR TOPIC MODELING
# ============================================================================

def preprocess_for_lda(text: str,
                       stop_words: set,
                       lemmatizer: WordNetLemmatizer,
                       min_word_length: int = 3) -> List[str]:
    """
    Preprocess text for LDA topic modeling.

    Args:
        text: Raw text
        stop_words: Set of stopwords to remove
        lemmatizer: NLTK lemmatizer
        min_word_length: Minimum word length to keep

    Returns:
        List of preprocessed tokens
    """
    if not isinstance(text, str) or not text.strip():
        return []

    # Tokenize
    try:
        tokens = word_tokenize(text.lower())
    except Exception:
        tokens = text.lower().split()

    # Filter and lemmatize
    processed = []
    for token in tokens:
        # Skip stopwords, short words, and non-alphabetic
        if (token not in stop_words and
            len(token) >= min_word_length and
            token.isalpha()):
            lemmatized = lemmatizer.lemmatize(token)
            processed.append(lemmatized)

    return processed


def prepare_corpus(df: pd.DataFrame, text_col: str = 'review_text_clean') -> Tuple:
    """
    Prepare corpus for LDA.

    Args:
        df: DataFrame with text column
        text_col: Name of text column

    Returns:
        Tuple of (texts, dictionary, corpus)
    """
    # Setup
    stop_words = set(stopwords.words('english')).union(CUSTOM_STOPWORDS)
    lemmatizer = WordNetLemmatizer()

    # Preprocess all texts
    logger.info("Preprocessing texts for topic modeling...")
    tqdm.pandas(desc="Preprocessing")

    texts = df[text_col].progress_apply(
        lambda x: preprocess_for_lda(x, stop_words, lemmatizer)
    ).tolist()

    # Remove empty documents
    texts = [t for t in texts if len(t) > 2]
    logger.info(f"Prepared {len(texts)} documents")

    # Create dictionary
    dictionary = corpora.Dictionary(texts)

    # Filter extremes
    dictionary.filter_extremes(no_below=5, no_above=0.5)
    logger.info(f"Dictionary size: {len(dictionary)} unique tokens")

    # Create corpus
    corpus = [dictionary.doc2bow(text) for text in texts]

    return texts, dictionary, corpus


# ============================================================================
# LDA MODEL TRAINING
# ============================================================================

def train_lda_model(corpus: List,
                    dictionary: corpora.Dictionary,
                    num_topics: int = 10,
                    passes: int = 15,
                    workers: int = 3) -> LdaMulticore:
    """
    Train LDA model.

    Args:
        corpus: Gensim corpus
        dictionary: Gensim dictionary
        num_topics: Number of topics
        passes: Number of passes through corpus
        workers: Number of worker threads

    Returns:
        Trained LDA model
    """
    logger.info(f"Training LDA model with {num_topics} topics...")

    lda_model = LdaMulticore(
        corpus=corpus,
        id2word=dictionary,
        num_topics=num_topics,
        random_state=42,
        passes=passes,
        workers=workers,
        chunksize=100,
        alpha='symmetric',
        eta='auto'
    )

    return lda_model


def find_optimal_topics(corpus: List,
                        dictionary: corpora.Dictionary,
                        texts: List[List[str]],
                        topic_range: List[int] = NUM_TOPICS_RANGE) -> Tuple[int, Dict]:
    """
    Find optimal number of topics using coherence score.

    Args:
        corpus: Gensim corpus
        dictionary: Gensim dictionary
        texts: Preprocessed texts
        topic_range: Range of topic numbers to try

    Returns:
        Tuple of (optimal_num_topics, coherence_scores)
    """
    logger.info("Finding optimal number of topics...")
    coherence_scores = {}

    for num_topics in tqdm(topic_range, desc="Testing topic counts"):
        model = train_lda_model(corpus, dictionary, num_topics, passes=10, workers=2)

        # Calculate coherence
        coherence_model = CoherenceModel(
            model=model,
            texts=texts,
            dictionary=dictionary,
            coherence='c_v'
        )
        coherence = coherence_model.get_coherence()
        coherence_scores[num_topics] = coherence

        logger.info(f"  Topics: {num_topics}, Coherence: {coherence:.4f}")

    # Find optimal
    optimal_topics = max(coherence_scores, key=coherence_scores.get)
    logger.info(f"Optimal number of topics: {optimal_topics} (coherence: {coherence_scores[optimal_topics]:.4f})")

    return optimal_topics, coherence_scores


# ============================================================================
# TOPIC ANALYSIS
# ============================================================================

def get_topic_keywords(lda_model: LdaMulticore,
                       num_words: int = 10) -> Dict[int, List[Tuple[str, float]]]:
    """
    Extract top keywords for each topic.

    Args:
        lda_model: Trained LDA model
        num_words: Number of top words per topic

    Returns:
        Dictionary mapping topic_id -> list of (word, weight) tuples
    """
    topics = {}
    for topic_id in range(lda_model.num_topics):
        topic_words = lda_model.show_topic(topic_id, num_words)
        topics[topic_id] = topic_words
    return topics


def assign_topics_to_documents(lda_model: LdaMulticore,
                               corpus: List,
                               df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign dominant topic to each document.

    Args:
        lda_model: Trained LDA model
        corpus: Gensim corpus
        df: Original DataFrame

    Returns:
        DataFrame with topic assignments
    """
    logger.info("Assigning topics to documents...")

    # Get topic distribution for each document
    topic_assignments = []

    for doc_bow in tqdm(corpus, desc="Assigning topics"):
        topic_dist = lda_model.get_document_topics(doc_bow)

        if topic_dist:
            # Get dominant topic
            dominant_topic = max(topic_dist, key=lambda x: x[1])
            topic_assignments.append({
                'dominant_topic': dominant_topic[0],
                'topic_probability': dominant_topic[1],
                'all_topics': dict(topic_dist)
            })
        else:
            topic_assignments.append({
                'dominant_topic': -1,
                'topic_probability': 0,
                'all_topics': {}
            })

    # Create DataFrame and merge
    # Note: corpus might be smaller than df due to filtering
    topics_df = pd.DataFrame(topic_assignments)

    return topics_df


def create_topic_labels(topic_keywords: Dict[int, List[Tuple[str, float]]]) -> Dict[int, str]:
    """
    Create descriptive labels for topics based on top keywords.

    Args:
        topic_keywords: Dictionary of topic -> keywords

    Returns:
        Dictionary of topic_id -> label
    """
    labels = {}
    for topic_id, keywords in topic_keywords.items():
        top_words = [word for word, _ in keywords[:3]]
        labels[topic_id] = f"Topic {topic_id}: {', '.join(top_words)}"
    return labels


# ============================================================================
# VISUALIZATION
# ============================================================================

def create_lda_visualization(lda_model: LdaMulticore,
                            corpus: List,
                            dictionary: corpora.Dictionary,
                            output_path: str):
    """
    Create interactive pyLDAvis visualization.

    Args:
        lda_model: Trained LDA model
        corpus: Gensim corpus
        dictionary: Gensim dictionary
        output_path: Path to save HTML visualization
    """
    try:
        logger.info("Creating LDA visualization...")

        vis_data = pyLDAvis.gensim_models.prepare(
            lda_model, corpus, dictionary, sort_topics=False
        )

        pyLDAvis.save_html(vis_data, output_path)
        logger.info(f"Saved visualization to: {output_path}")

    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")


# ============================================================================
# MAIN ANALYSIS FUNCTION
# ============================================================================

def run_topic_modeling(input_path: str,
                       output_dir: str = OUTPUT_DIR,
                       num_topics: int = None,
                       find_optimal: bool = True) -> Tuple[LdaMulticore, pd.DataFrame]:
    """
    Run complete topic modeling analysis.

    Args:
        input_path: Path to cleaned CSV file
        output_dir: Directory to save results
        num_topics: Number of topics (None to find optimal)
        find_optimal: Whether to search for optimal topic count

    Returns:
        Tuple of (trained model, DataFrame with topic assignments)
    """
    logger.info(f"Loading data from: {input_path}")
    df = pd.read_csv(input_path, encoding='utf-8')
    logger.info(f"Loaded {len(df)} reviews")

    os.makedirs(output_dir, exist_ok=True)

    # Determine text column
    text_col = 'review_text_clean' if 'review_text_clean' in df.columns else 'review_text'

    # Prepare corpus
    texts, dictionary, corpus = prepare_corpus(df, text_col)

    # Find optimal number of topics or use specified
    if find_optimal and num_topics is None:
        num_topics, coherence_scores = find_optimal_topics(corpus, dictionary, texts)

        # Save coherence scores
        pd.DataFrame([
            {'num_topics': k, 'coherence': v}
            for k, v in coherence_scores.items()
        ]).to_csv(os.path.join(output_dir, 'coherence_scores.csv'), index=False)
    elif num_topics is None:
        num_topics = 10

    # Train final model
    lda_model = train_lda_model(corpus, dictionary, num_topics, passes=20)

    # Get topic keywords
    topic_keywords = get_topic_keywords(lda_model, num_words=15)
    topic_labels = create_topic_labels(topic_keywords)

    # Save topic keywords
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    topics_df = pd.DataFrame([
        {
            'topic_id': topic_id,
            'label': topic_labels[topic_id],
            'keywords': ', '.join([f"{word} ({weight:.3f})" for word, weight in keywords])
        }
        for topic_id, keywords in topic_keywords.items()
    ])
    topics_df.to_csv(os.path.join(output_dir, f'topics_{timestamp}.csv'), index=False)

    # Print topics
    print_topics(topic_keywords, topic_labels)

    # Assign topics to documents
    topics_assigned = assign_topics_to_documents(lda_model, corpus, df)

    # Create visualization
    vis_path = os.path.join(output_dir, f'lda_visualization_{timestamp}.html')
    create_lda_visualization(lda_model, corpus, dictionary, vis_path)

    # Save model
    model_path = os.path.join(output_dir, f'lda_model_{timestamp}')
    lda_model.save(model_path)
    dictionary.save(os.path.join(output_dir, f'dictionary_{timestamp}.dict'))
    logger.info(f"Saved model to: {model_path}")

    return lda_model, topics_assigned


def print_topics(topic_keywords: Dict, topic_labels: Dict):
    """Print topics in formatted way."""
    logger.info("\n" + "=" * 60)
    logger.info("DISCOVERED TOPICS")
    logger.info("=" * 60)

    for topic_id, keywords in topic_keywords.items():
        logger.info(f"\n{topic_labels[topic_id]}")
        logger.info("-" * 40)
        for word, weight in keywords[:10]:
            logger.info(f"  {word}: {weight:.4f}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser(
        description="Run topic modeling for 'The For You Page Problem' research"
    )
    parser.add_argument('--input', type=str,
                       help='Path to input CSV file')
    parser.add_argument('--num-topics', type=int, default=None,
                       help='Number of topics (default: find optimal)')
    parser.add_argument('--no-optimize', action='store_true',
                       help='Skip finding optimal topic count')

    args = parser.parse_args()

    # Find input file
    if args.input:
        input_path = args.input
    else:
        # Find most recent cleaned file
        processed_files = glob.glob(os.path.join(INPUT_DIR, 'sentiment_analyzed_*.csv'))
        if not processed_files:
            processed_files = glob.glob(os.path.join(INPUT_DIR, 'cleaned_*.csv'))
        if not processed_files:
            logger.error(f"No processed CSV files found in {INPUT_DIR}")
            exit(1)
        input_path = max(processed_files, key=os.path.getctime)
        logger.info(f"Using most recent file: {input_path}")

    # Run topic modeling
    model, topics = run_topic_modeling(
        input_path,
        num_topics=args.num_topics,
        find_optimal=not args.no_optimize
    )

    logger.info("Done!")
