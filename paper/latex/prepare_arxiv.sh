#!/bin/bash
# =============================================================================
# Prepare arXiv Submission Package
# The "For You" Page Problem Research Paper
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARXIV_DIR="$SCRIPT_DIR/arxiv_package"
FIGURES_SRC="$SCRIPT_DIR/../../outputs/figures"

echo "=============================================="
echo "Preparing arXiv Submission Package"
echo "=============================================="
echo ""

# Clean and create arxiv directory
rm -rf "$ARXIV_DIR"
mkdir -p "$ARXIV_DIR/figures"

echo "[1/5] Copying LaTeX source files..."
cp "$SCRIPT_DIR/arxiv_submission.tex" "$ARXIV_DIR/main.tex"
cp "$SCRIPT_DIR/references.bib" "$ARXIV_DIR/"

echo "[2/5] Copying figures..."
# List of required figures (PDF preferred for arXiv)
FIGURES=(
    "platform_distribution"
    "sentiment_distribution"
    "sentiment_by_platform"
    "sentiment_by_age"
    "keyword_category_analysis"
    "correlation_heatmap"
    "wordcloud_all"
    "wordcloud_tiktok"
    "wordcloud_instagram"
    "wordcloud_youtube"
    "wordcloud_facebook"
    "wordcloud_positive"
    "wordcloud_negative"
    "wordcloud_explicit_content"
    "wordcloud_age_concerns"
    "wordcloud_mental_health"
    "wordcloud_algorithm"
    "wordcloud_parental_controls"
    "age_distribution"
)

for fig in "${FIGURES[@]}"; do
    if [ -f "$FIGURES_SRC/${fig}.pdf" ]; then
        cp "$FIGURES_SRC/${fig}.pdf" "$ARXIV_DIR/figures/"
        echo "  [OK] ${fig}.pdf"
    elif [ -f "$FIGURES_SRC/${fig}.png" ]; then
        cp "$FIGURES_SRC/${fig}.png" "$ARXIV_DIR/figures/"
        echo "  [OK] ${fig}.png"
    else
        echo "  [SKIP] ${fig} not found"
    fi
done

echo "[3/5] Creating README..."
cat > "$ARXIV_DIR/README.txt" << 'EOF'
arXiv Submission Package
========================

Title: The "For You" Page Problem: Explicit Content Exposure and Mental
       Health Concerns in Short-Form Video Platforms

Files:
------
- main.tex          : Main LaTeX source file
- references.bib    : BibTeX bibliography
- figures/          : All figures (PDF format)

Compilation:
------------
pdflatex main.tex
bibtex main
pdflatex main.tex
pdflatex main.tex

arXiv Categories:
-----------------
Primary: cs.SI (Social and Information Networks)
Secondary: cs.CY (Computers and Society), cs.CL (Computation and Language)

License:
--------
CC BY 4.0
EOF

echo "[4/5] Creating .tar.gz archive..."
cd "$SCRIPT_DIR"
tar -czvf arxiv_submission.tar.gz -C "$ARXIV_DIR" .

echo "[5/5] Verifying package..."
echo ""
echo "Package contents:"
tar -tvf arxiv_submission.tar.gz | head -30
echo ""

# Get file size
SIZE=$(du -h arxiv_submission.tar.gz | cut -f1)

echo "=============================================="
echo "SUCCESS: arXiv package created!"
echo "=============================================="
echo ""
echo "  Archive: $SCRIPT_DIR/arxiv_submission.tar.gz"
echo "  Size: $SIZE"
echo "  Directory: $ARXIV_DIR/"
echo ""
echo "To submit to arXiv:"
echo "  1. Go to https://arxiv.org/submit"
echo "  2. Upload arxiv_submission.tar.gz"
echo "  3. Select category: cs.SI"
echo "  4. Add metadata and submit"
echo ""
