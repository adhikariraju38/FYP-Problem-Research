# The "For You" Page Problem

## Explicit Content Exposure and Mental Health Concerns in Short-Form Video Platforms

**A Secondary Data Analysis from Nepal's Developing Digital Economy**

[![arXiv](https://img.shields.io/badge/arXiv-Preprint-red)](https://arxiv.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://python.org)

---

## Overview

This research project analyzes public discourse surrounding explicit content exposure, age-related concerns, and mental health implications of short-form video platforms (TikTok, Instagram Reels, YouTube Shorts, Facebook Reels) through large-scale secondary data analysis.

**Focus Region:** Nepal and developing digital economies in South Asia

### Key Findings

| Metric | Value |
|--------|-------|
| Total Records Collected | 221,764 |
| Relevant Records Analyzed | 8,435 |
| Negative Sentiment | 45.0% |
| Positive Sentiment | 42.3% |
| Most Discussed Platform | TikTok (49.6%) |
| Most Negative Platform | Instagram (M = -0.096) |
| Dominant Concern | Age/Child Safety (21.2%) |

---

## Project Structure

```
FYP-Problem-Research/
├── data/
│   ├── raw/                    # Raw scraped data
│   └── processed/              # Cleaned and analyzed data
├── scripts/
│   ├── 01a_scrape_playstore.py # Google Play Store scraper
│   ├── 01b_scrape_appstore.py  # Apple App Store scraper
│   ├── 01c_scrape_reddit.py    # Reddit scraper
│   ├── 01f_scrape_youtube.py   # YouTube comments scraper
│   ├── 01h_combine_all_data.py # Data combination
│   ├── 02_clean_data.py        # Data cleaning
│   ├── 03_sentiment_analysis.py # VADER sentiment analysis
│   ├── 04_topic_modeling.py    # LDA topic modeling
│   ├── 05_visualizations.py    # Generate figures
│   └── run_all.py              # Pipeline runner
├── outputs/
│   ├── figures/                # Publication-ready figures (PDF/PNG)
│   ├── topics/                 # LDA model and visualization
│   └── reports/                # Analysis reports
├── paper/
│   ├── draft_v1.md             # Markdown draft
│   └── latex/                  # LaTeX source for arXiv
│       ├── main.tex            # Full journal version
│       ├── arxiv_submission.tex # arXiv version
│       └── references.bib      # BibTeX references
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/adhikariraju38/FYP-Problem-Research.git
cd FYP-Problem-Research

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Download NLTK data
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet'); nltk.download('vader_lexicon')"
```

### 2. Run Data Collection (Optional - data included)

```bash
cd scripts
python 01a_scrape_playstore.py --max-reviews 10000
python 01b_scrape_appstore.py
python 01c_scrape_reddit.py
python 01f_scrape_youtube.py
python 01h_combine_all_data.py
```

### 3. Run Analysis Pipeline

```bash
cd scripts
python run_all.py --skip-scraping
```

Or run steps individually:
```bash
python 03_sentiment_analysis.py
python 04_topic_modeling.py
python 05_visualizations.py
```

### 4. Compile Paper

```bash
cd paper/latex
pdflatex main.tex && bibtex main && pdflatex main.tex && pdflatex main.tex
```

---

## Research Questions

1. **RQ1:** What is the overall sentiment of public discourse regarding explicit content concerns on short-form video platforms?
2. **RQ2:** How do sentiment patterns differ across platforms (TikTok, Instagram, YouTube, Facebook)?
3. **RQ3:** What are the dominant themes in user-generated content discussing platform concerns?
4. **RQ4:** What specific concerns do users express regarding children, algorithms, and mental health?
5. **RQ5:** What implications do these findings have for developing digital economies like Nepal?

---

## Data Sources

| Source | Records | Method |
|--------|---------|--------|
| Google Play Store | 160,000 | google-play-scraper API |
| Apple App Store | 12,086 | RSS feed scraping |
| Reddit | 48,606 | Public JSON API |
| YouTube Comments | 18,539 | youtube-comment-downloader |
| Trustpilot | 800 | Web scraping |
| **Total** | **221,764** | |

---

## Methodology

### Analysis Pipeline

```
Raw Data → Cleaning → Relevance Filtering → Sentiment Analysis → Topic Modeling → Visualization
   ↓          ↓              ↓                    ↓                   ↓              ↓
221,764    Dedupe      Keyword Match          VADER              LDA (k=5)      Figures
records    & Clean       (8,435)           Compound Score      Coherence=0.349   & Tables
```

### Keyword Categories

| Category | Keywords |
|----------|----------|
| Explicit Content | inappropriate, explicit, sexual, nudity, nsfw, porn |
| Age Concerns | kid, child, teen, minor, parent, underage |
| Mental Health | addiction, depression, anxiety, brain rot, dopamine |
| Algorithm | algorithm, fyp, for you page, recommended |
| Parental Controls | parental, control, filter, restrict, safety |

---

## Results Summary

### Sentiment by Platform

| Platform | n | Mean | SD |
|----------|---|------|-----|
| TikTok | 4,181 | +0.025 | 0.608 |
| Instagram | 1,011 | -0.096 | 0.639 |
| YouTube | 941 | -0.048 | 0.604 |
| Facebook | 648 | -0.076 | 0.624 |

### LDA Topics Identified

1. **Child Safety & Parental Concerns** - kid, child, parent, addiction
2. **Algorithm & Content Issues** - content, algorithm, view, platform
3. **Personal Usage Experiences** - year, day, time, friend
4. **Account Moderation & Banning** - account, banned, community, support
5. **Mental Health & Screen Time** - brain, dopamine, screen, attention

---

## Figures

| Figure | Description |
|--------|-------------|
| `platform_distribution.pdf` | Distribution of records by platform |
| `sentiment_distribution.pdf` | Overall sentiment distribution |
| `sentiment_by_platform.pdf` | Platform comparison |
| `sentiment_by_age.pdf` | Age group comparison |
| `keyword_category_analysis.pdf` | Keyword prevalence |
| `correlation_heatmap.pdf` | Variable correlations |
| `wordcloud_*.pdf` | Word clouds by platform/sentiment |

---

## Nepal Context

This research is particularly motivated by the unique challenges facing Nepal and South Asian developing economies:

- Internet penetration grew from 17% (2015) to 65% (2024)
- Youth (15-24) comprise ~20% of population
- Limited digital literacy education
- Nascent content moderation in local languages
- Regulatory frameworks still developing

---

## Citation

```bibtex
@article{fyp2026problem,
  author  = {[Author Name]},
  title   = {The "For You" Page Problem: Explicit Content Exposure and
             Mental Health Concerns in Short-Form Video Platforms},
  journal = {[Target Journal]},
  year    = {2026},
  note    = {A Secondary Data Analysis from Nepal's Developing Digital Economy}
}
```

---

## Target Publications

| Tier | Journal | Impact Factor |
|------|---------|---------------|
| 1 | Computers in Human Behavior | 9.9 |
| 1 | Cyberpsychology, Behavior, and Social Networking | 6.6 |
| 2 | Journal of Nepal Health Research Council | Free |
| 2 | Frontiers in Psychology | Fee waiver available |

---

## Ethics Statement

This research analyzes publicly available user reviews and social media posts. No IRB approval was required as data was publicly posted and anonymized before analysis. All identifiable information was removed. Research conducted in accordance with platform Terms of Service.

---

## License

MIT License - See [LICENSE](LICENSE) for details.

---

## Contact

- **Author:** [Author Name]
- **Email:** notrajuyadav@gmail.com
- **Location:** Kathmandu, Nepal

---

## Acknowledgments

This study was conducted as independent research from Nepal with no external funding, demonstrating that impactful research is possible even with limited resources. Thanks to the open-source community for the tools used in this research.
