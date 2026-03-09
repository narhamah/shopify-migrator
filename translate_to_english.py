#!/usr/bin/env python3
"""Step 2: Translate exported Spain content from Spanish to English.

Scrape-first approach: uses product data from the live Magento site as the
primary source (run scrape_kuwait.py first). Only translates content that
is NOT available from the scraped data — e.g., metafields, articles, FAQs.

Products are matched by SKU between Spain export and scraped data.

Resumable: saves progress after each batch.

Usage:
    python translate_to_english.py              # Full translation
    python translate_to_english.py --dry        # Show what would be translated
    python translate_to_english.py --model o3   # Use a different model
"""

import argparse

from dotenv import load_dotenv

from translate_gaps import (
    SPAIN_DIR,
    EN_DIR,
    BATCH_SIZE,
    TPM_LIMIT,
    translate_with_gaps,
)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Translate Spain export ES → EN (scrape-first)")
    parser.add_argument("--dry", action="store_true", help="Dry run: show fields without calling API")
    parser.add_argument("--model", default="gpt-5-mini", help="OpenAI model (default: gpt-5-mini)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"Fields per batch (default: {BATCH_SIZE})")
    parser.add_argument("--tpm", type=int, default=TPM_LIMIT, help=f"Tokens-per-minute budget (default: {TPM_LIMIT})")
    args = parser.parse_args()

    translate_with_gaps(
        source_dir=SPAIN_DIR,
        output_dir=EN_DIR,
        source_lang="Spanish",
        target_lang="English",
        lang_code="en",
        dry=args.dry,
        model=args.model,
        batch_size=args.batch_size,
        tpm=args.tpm,
    )


if __name__ == "__main__":
    main()
