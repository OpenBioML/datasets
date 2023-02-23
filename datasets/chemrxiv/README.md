# Chemrxiv dataset

### Download from 🤗 [HF Hub](https://huggingface.co/datasets/marianna13/chemrxiv)

### Dataset building & processing steps:
- Scrape metadata & PDF urls from [website](https://chemrxiv.org/engage/chemrxiv/public-dashboard)
- Download PDFs
- Parse PDFs using pyMuPDF, LayoutParser & Tesseract