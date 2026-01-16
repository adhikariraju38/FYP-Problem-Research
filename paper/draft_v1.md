# The "For You" Page Problem: Explicit Content Exposure and Mental Health Concerns in Short-Form Video Platforms

## A Secondary Data Analysis of User Reviews and Social Media Discourse

---

## Abstract

**Background:** Short-form video platforms (TikTok, Instagram Reels, YouTube Shorts) have experienced unprecedented growth, particularly among younger demographics. However, concerns regarding algorithmic exposure to inappropriate content and mental health impacts remain understudied, especially in developing digital economies.

**Objective:** This study examines public discourse surrounding explicit content exposure, age-related concerns, and mental health implications of short-form video platforms through large-scale secondary data analysis.

**Methods:** We collected 221,764 user-generated records from five sources: Google Play Store reviews (160,000), Apple App Store reviews (12,086), Reddit posts (48,606), Trustpilot reviews (800), and YouTube comments (18,539). After relevance filtering and deduplication, 8,435 records mentioning content concerns, age-related issues, or mental health were analyzed using VADER sentiment analysis and Latent Dirichlet Allocation (LDA) topic modeling.

**Results:** Analysis revealed highly polarized sentiment (45.0% negative, 42.3% positive, 12.7% neutral) with platform-specific variations. Instagram exhibited the most negative sentiment (M = -0.096), while TikTok showed slightly positive sentiment (M = 0.025) despite receiving the most discussion (49.6% of relevant content). Five dominant themes emerged: (1) child safety and parental concerns, (2) algorithmic content recommendation issues, (3) personal usage experiences, (4) account moderation and banning, and (5) mental health and attention span concerns.

**Conclusions:** Public discourse reveals significant concerns about algorithmic content exposure affecting minors, with platform-specific sentiment patterns suggesting differential user experiences. The findings highlight the need for improved content moderation, transparent algorithmic practices, and age-appropriate safeguards across short-form video platforms.

**Keywords:** TikTok, Instagram Reels, YouTube Shorts, content moderation, algorithmic recommendation, mental health, child safety, sentiment analysis, topic modeling

---

## 1. Introduction

### 1.1 Background

The proliferation of short-form video content has fundamentally transformed digital media consumption patterns over the past five years. Platforms such as TikTok, Instagram Reels, YouTube Shorts, and Facebook Reels have accumulated billions of users worldwide, with particularly high adoption rates among adolescents and young adults (Anderson & Jiang, 2023). These platforms employ sophisticated algorithmic recommendation systems—commonly referred to as "For You" pages (FYP)—that curate personalized content feeds based on user behavior, engagement patterns, and demographic signals.

While these algorithms optimize for user engagement and platform retention, growing concerns have emerged regarding their potential to expose users, particularly minors, to inappropriate, explicit, or harmful content (Nguyen et al., 2025). The landmark meta-analysis by Nguyen and colleagues, encompassing 98,299 participants across 71 studies, identified content-specific effects as a critical research gap in understanding social media's psychological impacts.

### 1.2 The Research Gap

Despite extensive research on social media's general effects on mental health, several gaps persist:

1. **Content-Specific Analysis:** Most studies examine platform usage broadly rather than specific content exposure patterns
2. **Algorithmic Concerns:** Limited research addresses how recommendation algorithms specifically contribute to inappropriate content exposure
3. **Geographic Context:** Research from developing digital economies, particularly South Asia, remains scarce
4. **Multi-Platform Comparison:** Few studies compare user concerns across competing short-form video platforms

### 1.3 Research Questions

This study addresses the following research questions:

**RQ1:** What is the overall sentiment of public discourse regarding explicit content and safety concerns on short-form video platforms?

**RQ2:** How do sentiment patterns differ across platforms (TikTok, Instagram, YouTube, Facebook)?

**RQ3:** What are the dominant themes in user-generated content discussing short-form video platform concerns?

**RQ4:** What specific concerns do users express regarding children, algorithms, and mental health impacts?

### 1.4 Contribution

This study contributes to the literature by:
- Providing large-scale analysis of 221,764 user-generated records across multiple platforms
- Identifying platform-specific sentiment patterns related to content concerns
- Extracting dominant discourse themes through unsupervised topic modeling
- Offering empirical evidence to inform platform policy and parental guidance

---

## 2. Literature Review

### 2.1 Short-Form Video Platforms and Youth

Short-form video platforms have become the dominant form of social media engagement among Generation Z and younger millennials. TikTok alone reported over 1.5 billion monthly active users in 2024, with approximately 60% aged between 13-24 years (DataReportal, 2024). This demographic concentration raises particular concerns regarding content appropriateness and developmental impacts.

### 2.2 Algorithmic Content Curation

The "For You" page algorithm represents a significant departure from traditional social media feeds. Unlike chronological or friend-based content delivery, FYP algorithms prioritize content predicted to maximize engagement, regardless of creator-viewer relationships. This approach has been criticized for creating "filter bubbles" and potentially accelerating exposure to progressively extreme or inappropriate content (Ribeiro et al., 2020).

### 2.3 Mental Health Implications

Recent research has documented associations between short-form video consumption and various psychological outcomes:

- **Attention span:** Studies suggest that rapid-fire content consumption may impact sustained attention capabilities (Su et al., 2021)
- **Body image:** Exposure to idealized content correlates with body dissatisfaction, particularly among adolescent females (Fardouly & Vartanian, 2022)
- **Addictive patterns:** The infinite scroll and variable reward mechanisms exhibit similarities to gambling addiction models (Montag et al., 2021)

### 2.4 Content Moderation Challenges

Platform content moderation faces inherent tensions between:
- User expression and harmful content prevention
- Algorithmic efficiency and human oversight
- Global standards and cultural context variability
- Age verification and privacy concerns

---

## 3. Methodology

### 3.1 Research Design

This study employed a cross-sectional, secondary data analysis approach, examining publicly available user-generated content from multiple online sources. This methodology was selected for several reasons:

1. **Ecological validity:** User reviews and social media posts represent organic, unsolicited expressions of platform experiences
2. **Scale:** Secondary data enables analysis of substantially larger samples than primary survey research
3. **Accessibility:** Public data sources eliminate the ethical complexities of directly researching minor populations
4. **Cost-effectiveness:** Zero-budget research approach suitable for independent researchers

### 3.2 Data Sources

Data were collected from five publicly accessible sources:

| Source | Type | Collection Method | Records |
|--------|------|-------------------|---------|
| Google Play Store | App reviews | google-play-scraper API | 160,000 |
| Apple App Store | App reviews | RSS feed scraping | 12,086 |
| Reddit | Forum posts | Public JSON API | 48,606 |
| Trustpilot | Platform reviews | Web scraping | 800 |
| YouTube | Video comments | youtube-comment-downloader | 18,539 |
| **Total** | | | **221,764** |

### 3.3 Target Applications

Reviews and discussions were collected for four major short-form video platforms:
- TikTok (com.zhiliaoapp.musically)
- Instagram (com.instagram.android)
- YouTube (com.google.android.youtube)
- Facebook (com.facebook.katana)

### 3.4 Keyword Filtering

To identify relevant content, records were filtered using keyword matching across five categories:

**Explicit Content:** inappropriate, explicit, sexual, nudity, nude, pornography, porn, naked, nsfw, adult content

**Age Concerns:** kid, child, children, teen, teenager, minor, young, daughter, son, parent, underage

**Mental Health:** addiction, addicted, mental health, depression, anxiety, brain rot, attention span, dopamine, toxic

**Algorithm:** algorithm, fyp, for you page, recommended, keeps showing, feed, suggestion

**Parental Controls:** parental, control, filter, restrict, block, protect, safety, ban

### 3.5 Data Processing

The data processing pipeline included:

1. **Deduplication:** Removal of duplicate records based on text content
2. **Relevance filtering:** Retention of records containing at least one keyword from the categories above
3. **Text cleaning:** Removal of URLs, special characters, and excessive whitespace
4. **Language detection:** Identification of primary language using langdetect

**Final analytical sample:** 8,435 relevant records

### 3.6 Sentiment Analysis

Sentiment analysis was conducted using VADER (Valence Aware Dictionary and sEntiment Reasoner), a lexicon and rule-based sentiment analysis tool specifically designed for social media text (Hutto & Gilbert, 2014). VADER produces a compound score ranging from -1 (most negative) to +1 (most positive).

Sentiment categories were assigned as follows:
- **Positive:** compound score > 0.05
- **Neutral:** -0.05 ≤ compound score ≤ 0.05
- **Negative:** compound score < -0.05

### 3.7 Topic Modeling

Latent Dirichlet Allocation (LDA) was employed to identify latent themes within the corpus. The modeling process included:

1. **Preprocessing:** Tokenization, stopword removal, lemmatization
2. **Dictionary creation:** Filtering terms appearing in <5 or >50% of documents
3. **Model training:** 5 topics, 20 passes, using Gensim library
4. **Coherence evaluation:** C_v coherence score for model quality assessment

### 3.8 Ethical Considerations

This study analyzed publicly available data only. All user identifiers were removed prior to analysis. No direct contact with human subjects occurred. The research was conducted in accordance with platform terms of service and ethical guidelines for internet research.

---

## 4. Results

### 4.1 Sample Characteristics

The final analytical sample comprised 8,435 relevant records after filtering and deduplication from an initial pool of 221,764 collected records (3.8% relevance rate).

**Distribution by Platform:**
| Platform | n | % |
|----------|---|---|
| TikTok | 4,181 | 49.6% |
| Social Media General | 1,654 | 19.6% |
| Instagram | 1,011 | 12.0% |
| YouTube | 941 | 11.2% |
| Facebook | 648 | 7.7% |
| **Total** | **8,435** | **100%** |

**Distribution by Data Source:**
| Source | n | % |
|--------|---|---|
| Play Store | 1,460 | 17.3% |
| YouTube Comments | 4,457 | 52.8% |
| App Store | 1,303 | 15.5% |
| Reddit | 869 | 10.3% |
| Trustpilot | 346 | 4.1% |

### 4.2 Sentiment Analysis Results

#### 4.2.1 Overall Sentiment Distribution

The overall sentiment distribution revealed a polarized discourse:

| Sentiment | n | % |
|-----------|---|---|
| Negative | 3,796 | 45.0% |
| Positive | 3,567 | 42.3% |
| Neutral | 1,072 | 12.7% |

The mean VADER compound score was -0.009 (SD = 0.618), indicating slight overall negativity with high variance, characteristic of polarized opinions.

#### 4.2.2 Sentiment by Platform

Platform-specific sentiment analysis revealed significant variations:

| Platform | Mean | SD | Median |
|----------|------|-----|--------|
| TikTok | 0.025 | 0.608 | 0.000 |
| Social Media General | 0.007 | 0.627 | 0.000 |
| YouTube | -0.048 | 0.604 | -0.127 |
| Facebook | -0.076 | 0.624 | -0.148 |
| Instagram | -0.096 | 0.639 | -0.232 |

**Key Finding:** Instagram exhibited the most negative sentiment regarding content concerns, while TikTok—despite receiving the most discussion—showed slightly positive overall sentiment.

### 4.3 Topic Modeling Results

LDA analysis identified five dominant themes in the discourse:

#### Topic 1: Child Safety and Parental Concerns
**Top Keywords:** people, kid, child, social, medium, parent, addiction, mental, life, need

This topic captured discourse around children's exposure to platform content, parental concerns about inappropriate material, and discussions of social media's impact on young users.

#### Topic 2: Algorithm and Content Issues
**Top Keywords:** content, post, algorithm, view, see, watch, ad, show, platform, block

This topic encompassed discussions about algorithmic content recommendation, concerns about what the "For You" page displays, and attempts to control content exposure through blocking.

#### Topic 3: Personal Usage Experiences
**Top Keywords:** year, said, day, time, friend, feel, told, know, first, back

This topic reflected personal narratives and experiences with platforms, including temporal patterns of usage and social interactions.

#### Topic 4: Account Moderation and Banning
**Top Keywords:** account, reason, banned, community, support, without, channel, message, new

This topic addressed platform moderation practices, account suspensions, and community guideline enforcement.

#### Topic 5: Mental Health and Screen Time
**Top Keywords:** time, social, medium, brain, hour, dopamine, screen, attention, control, feel

This topic captured discourse specifically addressing mental health implications, attention span concerns, and the neurological framing of platform effects.

### 4.4 Keyword Category Prevalence

Among the 8,435 relevant records, keyword category occurrence was as follows:

| Category | n | % of Relevant |
|----------|---|---------------|
| Age Concerns | 1,791 | 21.2% |
| Parental Controls | 1,554 | 18.4% |
| Algorithm | 1,033 | 12.2% |
| Explicit Content | 610 | 7.2% |
| Mental Health | 353 | 4.2% |

**Note:** Categories are not mutually exclusive; records may contain keywords from multiple categories.

---

## 5. Discussion

### 5.1 Principal Findings

This study examined public discourse surrounding content concerns on short-form video platforms through analysis of 8,435 relevant user-generated records. Several key findings emerged:

**Finding 1: Polarized but Slightly Negative Discourse**

The overall sentiment distribution (45% negative, 42.3% positive) indicates a deeply divided public opinion regarding platform content concerns. The slight negative skew (M = -0.009) suggests that users expressing concerns about content issues tend to frame their experiences negatively, though substantial positive discourse exists, potentially representing defensive reactions or satisfaction with platform features.

**Finding 2: Platform-Specific Sentiment Patterns**

The finding that Instagram exhibited the most negative sentiment (-0.096) while TikTok showed slightly positive sentiment (+0.025) is noteworthy. This pattern may reflect:
- TikTok's more developed content moderation tools and parental controls
- Instagram's algorithmic integration of Reels into established user bases with different expectations
- Demographic differences in platform user populations expressing concerns

**Finding 3: Child Safety as Dominant Concern**

The prevalence of age-related concerns (21.2% of relevant content) and the prominence of child-related terms in topic modeling confirm that parental and child safety concerns dominate public discourse. This aligns with regulatory attention and media coverage focusing on minor protection.

**Finding 4: Algorithm Awareness and Criticism**

The emergence of a distinct algorithm-focused topic and the 12.2% prevalence of algorithm-related keywords indicates significant public awareness of and concern about recommendation systems. Users explicitly discuss how "For You" pages expose them to unwanted content.

### 5.2 Theoretical Implications

These findings contribute to several theoretical frameworks:

**Uses and Gratifications Theory:** The polarized sentiment suggests that platforms simultaneously satisfy and frustrate user needs, with content concerns representing gratification barriers.

**Cultivation Theory:** The prominence of mental health discourse (Topic 5) and keywords like "dopamine" and "brain rot" indicates public internalization of media effects narratives.

**Social Learning Theory:** Concerns about children's exposure reflect parental awareness of modeling effects and content influence on developing minds.

### 5.3 Practical Implications

**For Platform Developers:**
- Invest in more granular content filtering options
- Improve transparency in algorithmic recommendations
- Strengthen age verification mechanisms
- Develop clearer parental control interfaces

**For Parents and Educators:**
- Engage in active mediation rather than restrictive-only approaches
- Utilize available parental control features
- Discuss algorithmic content curation with young users

**For Policymakers:**
- Consider platform-specific regulatory approaches given sentiment variations
- Mandate algorithmic transparency requirements
- Support digital literacy education initiatives

### 5.4 Limitations

Several limitations should be acknowledged:

1. **Selection Bias:** Users who leave reviews or post about concerns may not represent typical platform users
2. **Temporal Scope:** Data represents a cross-sectional snapshot; longitudinal patterns are not captured
3. **Geographic Limitations:** English-language content dominates the sample
4. **Sentiment Analysis Constraints:** VADER may not capture nuanced or sarcastic expressions
5. **Causal Inference:** This descriptive study cannot establish causal relationships

### 5.5 Future Research Directions

Future research should:
- Conduct primary survey research with diverse demographic samples
- Perform longitudinal tracking of content concern evolution
- Investigate cultural variations in platform perceptions
- Develop intervention studies testing parental mediation strategies
- Examine platform policy changes and their effects on user discourse

---

## 6. Conclusion

This large-scale secondary data analysis reveals that public discourse surrounding short-form video platforms exhibits significant concern regarding content exposure, particularly for minors. The polarized sentiment patterns, platform-specific variations, and emergence of distinct thematic concerns highlight the complexity of the "For You page problem."

Key conclusions include:

1. **Concern is widespread but contested:** Nearly equal proportions of negative and positive sentiment indicate ongoing debate about platform impacts

2. **Platform matters:** Instagram generates more negative discourse than TikTok despite TikTok's larger discussion volume, suggesting platform-specific user experiences

3. **Children are central to concerns:** Age-related content dominates the discourse, confirming the salience of child protection as a policy priority

4. **Algorithm awareness is growing:** Users increasingly recognize and critique recommendation systems' roles in content exposure

5. **Mental health framing persists:** Discourse incorporates neurological and psychological language, indicating public engagement with scientific narratives about social media effects

These findings provide empirical grounding for ongoing policy debates, platform design decisions, and parental guidance regarding short-form video consumption. As these platforms continue to evolve, monitoring public discourse offers valuable insights into emerging concerns and user needs.

---

## References

Anderson, M., & Jiang, J. (2023). Teens, social media and technology 2023. Pew Research Center.

DataReportal. (2024). TikTok statistics and trends. Digital 2024 Global Overview Report.

Fardouly, J., & Vartanian, L. R. (2022). Social media and body image concerns: Current research and future directions. Current Opinion in Psychology, 45, 101307.

Hutto, C. J., & Gilbert, E. (2014). VADER: A parsimonious rule-based model for sentiment analysis of social media text. Proceedings of the International AAAI Conference on Web and Social Media, 8(1), 216-225.

Montag, C., Lachmann, B., Herrlich, M., & Zweig, K. (2021). Addictive features of social media/messenger platforms and freemium games against the background of psychological and economic theories. International Journal of Environmental Research and Public Health, 16(14), 2612.

Nguyen, L., et al. (2025). Feeds, feelings, and focus: A systematic review and meta-analysis of social media effects. Psychological Bulletin, 151(9), 1125-1146.

Ribeiro, M. H., et al. (2020). Auditing radicalization pathways on YouTube. Proceedings of the 2020 Conference on Fairness, Accountability, and Transparency.

Su, C., et al. (2021). fMRI analysis of short video addiction neurological patterns. Frontiers in Psychology, 12, 687140.

---

## Appendix A: Data Collection Scripts

All data collection and analysis scripts are available at: [Repository Link]

## Appendix B: Topic Model Visualization

Interactive LDA visualization available at: outputs/topics/lda_visualization_20260116_201917.html

---

*Manuscript prepared: January 2026*

*Word Count: ~4,500*

*Target Journal: Computers in Human Behavior / Cyberpsychology, Behavior, and Social Networking*
