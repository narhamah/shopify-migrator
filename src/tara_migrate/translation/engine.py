"""Batch translation engine using OpenAI with TOON encoding.

Handles rich_text JSON safely by extracting text nodes, translating them
as plain text, and rebuilding the JSON structure with translated text.

Supports configurable models, batch sizes, and retry logic.

Usage:
    engine = TranslationEngine(developer_prompt, model="gpt-5-nano")
    t_map = engine.translate_fields(fields)  # fields = [{id, value}, ...]
    # t_map = {field_id: translated_value, ...}
"""

import json
import re
import time

from tara_migrate.core.rich_text import (
    extract_text_nodes,
    is_rich_text_json,
    rebuild,
    validate_structure,
)
from tara_migrate.translation.toon import DELIM, from_toon, to_toon


class TranslationEngine:
    """Batch translation engine with rich_text-safe handling."""

    def __init__(
        self,
        developer_prompt,
        model="gpt-5-nano",
        reasoning_effort="minimal",
        batch_size=80,
        max_retries=3,
    ):
        self.developer_prompt = developer_prompt
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.batch_size = batch_size
        self.max_retries = max_retries
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import openai
            self._client = openai.OpenAI()
        return self._client

    def translate_fields(self, fields, progress_callback=None):
        """Translate a list of {id, value} dicts, handling rich_text safely.

        Returns a dict of {field_id: translated_value}.
        Rich_text JSON fields are decomposed into text nodes, translated
        individually, and rebuilt with the original JSON structure.

        Args:
            fields: List of {id, value} dicts.
            progress_callback: Optional fn(translated_count, total_count).
        """
        # Separate rich_text JSON from plain text
        plain_fields = []
        rich_text_map = {}  # field_id → {parsed, texts}

        for field in fields:
            fid = field["id"]
            value = field["value"]

            if is_rich_text_json(value):
                texts, parsed = extract_text_nodes(value)
                if texts:
                    rich_text_map[fid] = {"parsed": parsed, "texts": texts}
                    for idx, (path, text_val) in enumerate(texts):
                        if text_val and text_val.strip():
                            plain_fields.append({
                                "id": f"{fid}__RT_{idx}",
                                "value": text_val,
                            })
                    continue

            plain_fields.append({"id": fid, "value": value})

        # Translate all plain text in batches
        t_map = {}
        all_missing = []
        total = len(plain_fields)
        for i in range(0, total, self.batch_size):
            batch = plain_fields[i:i + self.batch_size]
            batch_map = self._translate_batch(batch)
            t_map.update(batch_map)
            # Track missing fields for retry
            batch_ids = {f["id"] for f in batch}
            missing = [f for f in batch if f["id"] not in batch_map]
            all_missing.extend(missing)
            if progress_callback:
                progress_callback(len(t_map), total)
            if i + self.batch_size < total:
                time.sleep(1)

        # Retry missing fields in a single focused batch
        if all_missing:
            print(f"    Retrying {len(all_missing)} missing fields...")
            retry_map = self._translate_batch(all_missing)
            t_map.update(retry_map)
            still_missing = len(all_missing) - len(retry_map)
            if still_missing:
                print(f"    WARNING: {still_missing} fields still missing after retry")

        # Rebuild rich_text JSON with translated nodes
        for fid, rt_info in rich_text_map.items():
            translations = {}
            for idx, (path, text_val) in enumerate(rt_info["texts"]):
                sub_id = f"{fid}__RT_{idx}"
                ar_text = t_map.get(sub_id)
                if ar_text:
                    translations[tuple(path)] = ar_text
            if translations:
                rebuilt = rebuild(rt_info["parsed"], translations)
                # Validate structure against original (restores listType, etc.)
                original_json = json.dumps(rt_info["parsed"], ensure_ascii=False)
                t_map[fid] = validate_structure(rebuilt, original_json)

        # Clean up internal __RT_ keys from the returned map
        return {k: v for k, v in t_map.items() if "__RT_" not in k}

    def _translate_batch(self, fields):
        """Translate a batch of plain text {id, value} dicts via TOON."""
        toon_input = to_toon(fields)
        user_message = (
            "Translate the following TOON input and return TOON only.\n"
            "IMPORTANT: Translate ALL ingredient names, benefit names, "
            "and category labels. Keep INCI/scientific names as-is.\n\n"
            f"<TOON>\n{toon_input}\n</TOON>"
        )

        print(f"    Translating {len(fields)} fields "
              f"({self.model}, reasoning={self.reasoning_effort})...")

        for attempt in range(self.max_retries):
            try:
                kwargs = {
                    "model": self.model,
                    "instructions": self.developer_prompt,
                    "input": user_message,
                }
                if self.model.startswith("o") or "nano" in self.model:
                    kwargs["reasoning"] = {"effort": self.reasoning_effort}

                response = self.client.responses.create(**kwargs)

                result = ""
                for item in response.output:
                    if item.type == "message":
                        for content in item.content:
                            if content.type == "output_text":
                                result += content.text

                result = result.strip()
                # Strip code fences
                if result.startswith("```"):
                    lines = result.split("\n")
                    if lines[-1].strip() == "```":
                        result = "\n".join(lines[1:-1])
                    else:
                        result = "\n".join(lines[1:])
                result = re.sub(r"</?TOON>", "", result).strip()

                translated = from_toon(result)
                t_map = {entry["id"]: entry["value"] for entry in translated}

                matched = len(set(f["id"] for f in fields) & set(t_map.keys()))
                tokens = response.usage.input_tokens + response.usage.output_tokens
                print(f"    Got {matched}/{len(fields)} translations ({tokens} tokens)")
                return t_map

            except Exception as e:
                print(f"    Error (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** (attempt + 1))

        return {}


def load_developer_prompt(prompt_path, fallback=None):
    """Load a developer/system prompt from file.

    If the prompt file doesn't exist, tries to build it from the Arabic TOV
    file (tara_tov_ar.txt) found in the project root or parent directories.

    Args:
        prompt_path: Path to the prompt text file.
        fallback: Optional fallback prompt if file not found.

    Returns the prompt string.
    """
    import os
    if os.path.exists(prompt_path):
        with open(prompt_path, "r", encoding="utf-8") as f:
            prompt = f.read()
        print(f"Loaded developer prompt ({len(prompt):,} chars)")
        return prompt

    # Try to build from TOV file
    tov_prompt = _build_developer_prompt_from_tov(prompt_path)
    if tov_prompt:
        return tov_prompt

    if fallback:
        print(f"WARNING: Prompt not found at {prompt_path}, using fallback")
        return fallback

    default = (
        "You are a translation engine for a Shopify e-commerce store. "
        "Translate English to the target language. Return TOON format only. "
        "Translate ALL ingredient names and category labels. "
        "Keep INCI/scientific names as-is."
    )
    print(f"WARNING: Prompt not found at {prompt_path}, using default")
    return default


def _build_developer_prompt_from_tov(prompt_path):
    """Build and cache the developer prompt from the Arabic TOV file.

    Searches for tara_tov_ar.txt in parent directories, combines it with
    translation instructions, and saves to prompt_path for future runs.

    Returns the prompt string, or None if TOV file not found.
    """
    import os

    # Search for TOV file in parent directories
    tov_file = None
    search_dir = os.path.dirname(os.path.abspath(prompt_path))
    for _ in range(5):  # up to 5 levels
        candidate = os.path.join(search_dir, "tara_tov_ar.txt")
        if os.path.exists(candidate):
            tov_file = candidate
            break
        parent = os.path.dirname(search_dir)
        if parent == search_dir:
            break
        search_dir = parent

    if not tov_file:
        return None

    with open(tov_file, "r", encoding="utf-8") as f:
        tov_text = f.read()

    prompt = (
        "You are a professional Arabic translator for TARA, a luxury scalp-care brand.\n"
        "You translate English (or Spanish) product content into Modern Standard Arabic "
        "for a Gulf audience (Saudi Arabia, Kuwait, UAE).\n\n"
        "## Rules\n"
        "- Return TOON format only: each line is `numeric_id§translated_value`\n"
        "- Keep numeric IDs exactly as given — never translate or modify them\n"
        "- Translate ALL content values into Arabic\n"
        "- Keep INCI/scientific names as-is (e.g., Sodium Hyaluronate, Tocopherol)\n"
        "- Keep brand name \"TARA\" as-is, never translate it\n"
        "- Keep product names as-is (e.g., Kansa Wand, Gua Sha)\n"
        "- Translate ingredient common names into Arabic "
        "(e.g., Rosemary → إكليل الجبل, Onion → البصل)\n"
        "- For rich_text JSON: translate only text content, preserve all JSON structure\n"
        "- Use right-to-left punctuation where appropriate\n\n"
        "## TARA Arabic Tone of Voice\n\n"
        f"{tov_text}\n"
    )

    # Cache for future runs
    os.makedirs(os.path.dirname(prompt_path) or ".", exist_ok=True)
    with open(prompt_path, "w", encoding="utf-8") as f:
        f.write(prompt)
    print(f"Built developer prompt from {tov_file} ({len(prompt):,} chars) → saved to {prompt_path}")

    return prompt
