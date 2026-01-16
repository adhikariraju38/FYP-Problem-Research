#!/bin/bash
# =============================================================================
# LaTeX Compilation Script
# The "For You" Page Problem Research Paper
# =============================================================================

set -e  # Exit on error

MAIN="main"

echo "=============================================="
echo "Compiling: The 'For You' Page Problem Paper"
echo "=============================================="
echo ""

# Check if pdflatex is installed
if ! command -v pdflatex &> /dev/null; then
    echo "ERROR: pdflatex not found!"
    echo "Please install LaTeX. On Ubuntu/Debian:"
    echo "  sudo apt install texlive-latex-recommended texlive-latex-extra texlive-fonts-recommended"
    exit 1
fi

# Check if bibtex is installed
if ! command -v bibtex &> /dev/null; then
    echo "ERROR: bibtex not found!"
    echo "Please install full LaTeX distribution."
    exit 1
fi

# First pass
echo "[1/4] First LaTeX pass..."
pdflatex -interaction=nonstopmode "$MAIN.tex" > /dev/null 2>&1 || {
    echo "ERROR: First pass failed. Running verbose mode..."
    pdflatex "$MAIN.tex"
    exit 1
}

# BibTeX
echo "[2/4] Running BibTeX..."
bibtex "$MAIN" > /dev/null 2>&1 || {
    echo "WARNING: BibTeX had warnings/errors. Check $MAIN.blg for details."
}

# Second pass
echo "[3/4] Second LaTeX pass..."
pdflatex -interaction=nonstopmode "$MAIN.tex" > /dev/null 2>&1

# Third pass
echo "[4/4] Third LaTeX pass (final)..."
pdflatex -interaction=nonstopmode "$MAIN.tex" > /dev/null 2>&1

echo ""
echo "=============================================="
echo "SUCCESS: $MAIN.pdf created!"
echo "=============================================="
echo ""

# Show file size
if [ -f "$MAIN.pdf" ]; then
    SIZE=$(du -h "$MAIN.pdf" | cut -f1)
    PAGES=$(pdfinfo "$MAIN.pdf" 2>/dev/null | grep Pages | awk '{print $2}' || echo "unknown")
    echo "  File: $MAIN.pdf"
    echo "  Size: $SIZE"
    echo "  Pages: $PAGES"
fi

# Optional: Open the PDF
if [ "$1" == "--view" ]; then
    echo ""
    echo "Opening PDF..."
    if command -v xdg-open &> /dev/null; then
        xdg-open "$MAIN.pdf"
    elif command -v evince &> /dev/null; then
        evince "$MAIN.pdf"
    elif command -v okular &> /dev/null; then
        okular "$MAIN.pdf"
    fi
fi
