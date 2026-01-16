# =============================================================================
# Makefile for LaTeX compilation
# The "For You" Page Problem Research Paper
# =============================================================================

# Main document name (without extension)
MAIN = main

# LaTeX compiler (pdflatex or xelatex)
LATEX = pdflatex

# BibTeX
BIBTEX = bibtex

# Output directory
OUTPUT = .

# Figures directory (relative to this directory)
FIGURES = ../../outputs/figures

# =============================================================================
# Targets
# =============================================================================

.PHONY: all pdf clean view help

# Default target: build PDF
all: pdf

# Build PDF with bibliography
pdf: $(MAIN).pdf

$(MAIN).pdf: $(MAIN).tex references.bib
	@echo "=== First LaTeX pass ==="
	$(LATEX) $(MAIN).tex
	@echo "=== Running BibTeX ==="
	$(BIBTEX) $(MAIN)
	@echo "=== Second LaTeX pass ==="
	$(LATEX) $(MAIN).tex
	@echo "=== Third LaTeX pass (final) ==="
	$(LATEX) $(MAIN).tex
	@echo ""
	@echo "=== Build complete: $(MAIN).pdf ==="

# Quick build (no bibliography update)
quick:
	$(LATEX) $(MAIN).tex

# Clean auxiliary files
clean:
	rm -f $(MAIN).aux $(MAIN).log $(MAIN).bbl $(MAIN).blg
	rm -f $(MAIN).toc $(MAIN).lof $(MAIN).lot
	rm -f $(MAIN).out $(MAIN).nav $(MAIN).snm
	rm -f $(MAIN).fdb_latexmk $(MAIN).fls $(MAIN).synctex.gz
	@echo "Cleaned auxiliary files"

# Clean everything including PDF
cleanall: clean
	rm -f $(MAIN).pdf
	@echo "Cleaned all files including PDF"

# View PDF (Linux)
view: pdf
	xdg-open $(MAIN).pdf 2>/dev/null || evince $(MAIN).pdf 2>/dev/null || echo "No PDF viewer found"

# Check if figures exist
check-figures:
	@echo "Checking required figures..."
	@for fig in platform_distribution sentiment_distribution sentiment_by_platform \
	            wordcloud_all wordcloud_tiktok wordcloud_instagram wordcloud_youtube \
	            wordcloud_positive wordcloud_negative correlation_heatmap \
	            keyword_category_analysis sentiment_by_age; do \
		if [ -f "$(FIGURES)/$$fig.pdf" ]; then \
			echo "  [OK] $$fig.pdf"; \
		elif [ -f "$(FIGURES)/$$fig.png" ]; then \
			echo "  [OK] $$fig.png (consider converting to PDF)"; \
		else \
			echo "  [MISSING] $$fig"; \
		fi; \
	done

# Help
help:
	@echo "LaTeX Makefile for 'The For You Page Problem' paper"
	@echo ""
	@echo "Usage:"
	@echo "  make          - Build PDF (full compilation with bibliography)"
	@echo "  make pdf      - Same as above"
	@echo "  make quick    - Quick build (single pass, no bibliography)"
	@echo "  make clean    - Remove auxiliary files"
	@echo "  make cleanall - Remove all generated files including PDF"
	@echo "  make view     - Build and open PDF"
	@echo "  make check-figures - Check if required figures exist"
	@echo "  make help     - Show this help message"
