# Tara Arabic Translation API Usage

## User Message Format

```
Translate the following TOON input into Tara Arabic and return TOON only.

<TOON>
{{TOON_INPUT}}
</TOON>
```

## Minimal Responses API Shape

```json
{
  "model": "gpt-5-nano",
  "prompt_cache_key": "tara-ar-translation-v1",
  "input": [
    {
      "role": "developer",
      "content": "PASTE THE CACHED DEVELOPER PROMPT FROM tara_cached_developer_prompt.txt HERE"
    },
    {
      "role": "user",
      "content": "Translate the following TOON input into Tara Arabic and return TOON only.\n\n<TOON>\n{{TOON_INPUT}}\n</TOON>"
    }
  ]
}
```

## Notes

- The developer prompt is cached via `prompt_cache_key: "tara-ar-translation-v1"`.
- The full developer prompt lives in `Arabic/tara_cached_developer_prompt.txt`.
- TOON payloads go in the user message, wrapped in `<TOON>` tags.
- The model returns valid TOON only — no explanations, no extras.