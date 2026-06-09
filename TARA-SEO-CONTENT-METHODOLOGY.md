# Tara — SEO & Content Methodology

**Brief for the content / SEO agent.** This is how Tara writes every customer-facing word: PDPs, metafields, ingredient pages, FAQs, collection copy, blog/GEO articles, and SEO tags. It is not a style suggestion — it is the operating doctrine. Read it fully before writing a single line, and keep it open while you work.

> **The one rule, if you remember nothing else:** *Every performance statement traces to a peer-reviewed receipt, written at or below the tier that receipt supports, at the dose the study used — and every ingredient also says what it does NOT do.* If you can't cite it, you can't claim it. **"Tara — what we know works."**

---

## 1. Who Tara is (identity locks — never violate)

- **A Spanish hair-science house. Lives in Barcelona.** Not Gulf, not Kuwaiti, not "founder-led." **The lab is the face** — no founder narrative, no personal story.
- **Positioning line:** *"Tara — what we know works."*
- **Not** a "natural" brand. **Not** a "biotech" brand. The wedge is **peer-reviewed science, plainly stated, with the receipts shown — and the honesty to say what doesn't work.** That honesty is the moat.
- **Register:** refined, elegant, dignified. **No slogans, no hype, no cheese, no sales-y exclamation.** Restraint signals confidence. Conviction survives; the delivery is elevated.
- **Audience-neutral:** never gender the reader or the buyer. Say *people, subjects, hair* — never *women/men*. (See §6 for the trial-design exception — it is critical.)

---

## 2. The non-negotiable doctrine (the gates every piece must clear)

| # | Gate | What it means for your copy |
|---|---|---|
| 1 | **Peer-reviewed, no bullshit** | Every efficacy statement maps to a real study in the Proof Engine. Never assert a number, mechanism, or outcome from imagination. |
| 2 | **Evidence tiers (A/B/C/D)** | Write at or below the tier the evidence supports. **Tier-D never ships.** Tier-C = mechanism language only (see §4). |
| 3 | **Dose gating** | A receipt only licenses a claim **at or above the dose the study used.** Below the gate → soften to mechanism (see §4). |
| 4 | **The un-claim** | Every ingredient/product piece states **what it does NOT do**, with the same rigour as what it does. This is mandatory, not optional. |
| 5 | **Supplier catalogues ≠ receipts** | Croda/Sederma/Lucas-Meyer marketing is **not evidence.** A manufacturer study may be cited **only if disclosed as manufacturer-funded.** Independent Scopus/PubMed is the real receipt. |
| 6 | **Gender-honest** | Gender-neutral to the reader; but **never relabel a single-sex trial** (e.g. a 100-men study) as "subjects/people." Present it by its real design. |
| 7 | **Cite precisely** | On creative/PDPs: short form **"Author, Journal Year"** (e.g. *Panahi, Skinmed 2015*). PMID/DOI live **only on the science page**, never inline on a PDP. |

---

## 3. Your source of truth: the Proof Engine

**You do not invent claims. You retrieve them.** Everything you may say is already graded and sitting in the Proof Engine at `C:\Users\narha\Dropbox\Tara\_proof-engine\`. Read these before writing:

| File | What it gives you |
|---|---|
| `content-output/LIVE-COLLECTION-CLAIMS.json` / `.md` | **Start here.** Per live collection: the graded **hero claim + un-claim + short citation + tier**. This is the spine — most PDP copy inherits directly from it. |
| `content-output/CORPUS-MASTER.md` | Full ingredient corpus, every audited claim, tiers, caveats. |
| `content-output/ingredients-page-LIVE.md` | Customer-facing, live-only ingredient master (already corrected). |
| `science-hub/` (`*.html` + `llms.txt` + `robots.txt`) | The deployable science pages — the **template** for ingredient/blog content (Headline → Story → does → doesn't → References → JSON-LD). |
| `content-output/FOUNDER-DECISIONS.md` | **Open calls you must respect** (see §12) — e.g. claims that were removed and may NOT be re-introduced. |

**When you are unsure whether a claim is defensible at a given tier or dose, ask the Tara Formulation Engine** (`C:\Users\narha\Dropbox\Tara\tara-formulator`): `npm run ask -- "Can I claim X for the Detox shampoo's salicylic acid at 0.4%?"`. It crosses the DB + Scopus + chemistry guides and returns the tier, the dose gate, and the honest un-claim. Use it as your fact-checker.

---

## 4. Tiers + dose gating — the rule that decides what verb you may use

**Tiers**
- **A** — independent RCT / strong human evidence → you may state the outcome directly ("matched 2% minoxidil in a controlled trial").
- **B** — independent but weaker design, or strong in-vitro mechanism with independent support → state the mechanism + the specific study, hedged to its design.
- **C** — in-vitro / mechanism only → **mechanism language only** ("supports", "is associated with", "in lab studies"). No efficacy promise.
- **D** — anecdotal / supplier-only → **does not ship. Do not write it at all.**

**Dose gating — known gates (verbatim, respect them):**
- **Capixyl (red clover + Acetyl Tetrapeptide-3):** the supplier clinical applies only **≥3%**. Below 3% → "peptide-supported", no efficacy claim.
- **Salicylic acid (Detox TA08):** ships at **0.4%**; anti-dandruff needs **≥1.8%**. → **No "anti-dandruff / controls dandruff" verb.** Only "lifts scalp build-up / exfoliates."
- **Bakuchiol (TA10):** retinol head-to-head holds only **≥0.5%**. Below → "retinoid-pathway support."
- **Niacinamide:** **skin barrier/tone only.** It does **not** build keratin; it inhibited hair-follicle growth ex-vivo. **Never attach a hair-growth or keratin claim to it.**

---

## 5. The three-layer voice

Write every substantive piece in three registers, layered — not three separate paragraphs, but three things the copy does at once.

1. **Performance layer — receipt + verb + period.** The headline claim, stated flat, with the short citation. *"Rosemary oil matched 2% minoxidil over six months. (Panahi, Skinmed 2015)."* No adjectives doing the work the data should do.
2. **Story layer (Sagan / Gladwell).** Beneath the headline, tell the *experiment* as a short narrative — what they did, what they measured, what surprised them. Trigger the satisfaction of understanding. Citations sit at the **bottom of the page**, not inline.
3. **Refined register.** Elevated, unhurried, dignified diction throughout. Cut every exclamation mark, every "amazing/powerful/revolutionary," every stacked superlative.

**Verbs to use:** matched, reduced, increased, inhibited, raised, measured, demonstrated, was tested against.
**Mumble to avoid:** nourishes, revitalises, boosts, unlocks, harnesses, supercharges, miracle, secret, transforms (unless an instrument measured the transformation).

---

## 6. The un-claim protocol (mandatory on every ingredient/product)

Every piece must, somewhere, **state plainly what the ingredient does NOT do** — the benefit people wrongly assume — with a reason. This is the single most distinctive thing Tara does. Examples already grounded:

- **Silverfree (Sage serum):** defends the pigment you still make; **it does not reverse grey.** (Greying is oxidative + stem-cell loss; the stem-cell part is irreversible.)
- **Niacinamide:** does not build keratin; it's a skin-barrier active.
- **Charcoal (Detox):** lifts surface build-up; **does not "detox" the follicle.**
- **Onion:** the 87% figure is **alopecia areata** (an autoimmune condition), **not** general thinning — don't transplant it.

**The trial-design honesty rule (critical):** if the only receipt is single-sex (e.g. Panahi was 100 men), **do not** launder it into "subjects." Present it by its actual design ("a controlled, head-to-head trial") and let the reader judge generalisability. Never imply universality the study didn't test.

---

## 7. Mapping the methodology onto the Shopify content surfaces

This is the concrete part. Each field below has a job and a tier ceiling.

### Product (PDP) metafields
| Field | What goes in it | Doctrine |
|---|---|---|
| `tagline` | One refined line — the product's single truest claim. | Tier-A/B outcome or honest mechanism. No hype. |
| `short_description` | 1–2 sentences: what it is + the headline receipt. | Performance layer + short citation. |
| `key_benefits` | 3–4 bullets, each a graded benefit. | Each bullet ≤ its tier. Mechanism verbs for C. |
| `clinical_results` | The actual study/data, stated by design. | Independent receipt first; manufacturer study **only if disclosed**. Short cite. |
| `whats_inside` | The actives + their evidenced role. | Pull roles from `ingredients-page-LIVE.md`. |
| `free_of` | What it excludes. | Factual only — don't imply the excluded thing is dangerous. |
| `how_to_use` | Directions. | Plain. |
| `title_tag` (SEO) | ≤60 char, primary query + brand. | Honest; no claim you can't back. |
| `description_tag` (SEO) | ≤155 char meta. | The receipt, compressed, for the SERP/AI snippet (see §8). |
| `ingredient_refs` / `faq_refs` | Links to ingredient metaobjects + FAQs. | Keep them populated — they feed schema + internal linking. |

### Ingredient metaobjects (34 entries)
Fields `name, inci_name, benefits, description, source, origin, category, concern`. Write `benefits`/`description` **straight from the Proof Engine ingredient record**, at tier, **with the un-claim included in `description`.** `inci_name` must be exact.

### FAQ entries (122) → FAQPage schema
Each FAQ is a passage-level citable unit (see §8). Answer in 2–4 sentences, self-contained, with the receipt. Great FAQ targets: *"Does rosemary actually regrow hair?"*, *"Does Tara's serum reverse grey?"* (answer honestly — that's the un-claim doing SEO work).

### Collection descriptions
Lead with the collection's grounded hero claim from `LIVE-COLLECTION-CLAIMS`. One paragraph, refined, with the short cite.

### Blog / GEO articles
Follow the **science-hub template** exactly: **Headline → Story (the experiment as narrative) → What it does → What it doesn't → References (with PMID/DOI) → JSON-LD.** This is your highest-leverage AI-search surface.

---

## 8. GEO / AI-search layer (how this content gets cited by AI + ranks)

Search and AI answer-engines are the distribution. Optimise for **being quoted**, not just ranked.

- **Passage-level citability:** write self-contained, factual passages an AI can lift verbatim and attribute. Short sentences. One claim per sentence. The receipt adjacent to the claim.
- **JSON-LD on science/ingredient pages:** `MedicalScholarlyArticle` (for the evidence) **+** `FAQPage` (for the Q&A). Mirror the on-page references in the schema `citation` field.
- **`description_tag`** is your AI-snippet bait: lead with the receipt ("Matched 2% minoxidil in a 6-month trial"), not adjectives.
- **`llms.txt` + crawler-open `robots.txt`:** keep GPTBot/ClaudeBot/PerplexityBot allowed; keep `llms.txt` pointing at the science hub. (Templates in `_proof-engine/science-hub/`.)
- **Citation format discipline:** short "Author, Journal Year" on PDPs/creative; **full PMID/DOI only on the science/ingredient pages and in JSON-LD.**

---

## 9. The per-piece workflow (do this every time)

1. **Identify** the product/ingredient/collection and pull its record from `LIVE-COLLECTION-CLAIMS.json` (+ `ingredients-page-LIVE.md`).
2. **Read the tier + dose gate.** Decide the strongest verb you're allowed (§4).
3. **Check FOUNDER-DECISIONS.md** — is anything here removed/restricted? (§12)
4. **Draft** in the three-layer voice (§5): receipt + verb + period, then the story, refined throughout.
5. **Write the un-claim** (§6). No piece ships without it.
6. **Cite** — short form on the page; full PMID/DOI only if it's a science page.
7. **Fill the right metafields** (§7) — don't overload `tagline` with what belongs in `clinical_results`.
8. **Add schema** if it's a science/ingredient/FAQ surface (§8).
9. **Run the QA checklist** (§11). If unsure on any claim, **ask the Formulation Engine** (§3).

---

## 10. Worked example — Rosemary Remedy (PDP + ingredient)

- **tagline:** *Rosemary oil, measured against the standard.*
- **short_description:** *In a six-month controlled trial, rosemary oil matched 2% minoxidil for hereditary thinning — with less scalp itch. (Panahi, Skinmed 2015.)*
- **key_benefits:** • Matched 2% minoxidil over 6 months *(controlled trial)* • Mechanism: supports follicular circulation *(in-vitro)* • Less scalp itching than the comparator.
- **clinical_results:** *A controlled, head-to-head trial compared rosemary oil to 2% minoxidil over six months and found no significant difference in hair count, with rosemary causing less scalp itch (Panahi, Skinmed 2015).*
- **un-claim (in ingredient `description`):** *Rosemary supports the scalp environment; it is not an overnight regrowth cure, and the trial measured maintenance and modest regain over months, not weeks.*
- **science page:** full story + References with PMID + `MedicalScholarlyArticle`/`FAQPage` JSON-LD.

---

## 11. Pre-publish QA checklist (every piece must pass)

- [ ] Every performance claim traces to a Proof-Engine receipt (no invented numbers/mechanisms).
- [ ] Written **at or below** the evidence tier; no Tier-D content; Tier-C = mechanism verbs only.
- [ ] **Dose gate respected** (Capixyl ≥3%, salicylic <1.8% = no dandruff verb, bakuchiol ≥0.5%, niacinamide skin-only).
- [ ] **Un-claim present** — the piece says what it does NOT do.
- [ ] Gender-neutral to the reader; **single-sex trials not relabelled** as "subjects."
- [ ] Manufacturer studies **disclosed** as such; supplier marketing not treated as evidence.
- [ ] Citation format: short "Author, Journal Year" on PDP; PMID/DOI only on science page + JSON-LD.
- [ ] Refined register: no exclamation marks, no hype verbs, no stacked superlatives.
- [ ] Right metafield for the right content; `ingredient_refs`/`faq_refs` populated.
- [ ] Schema added on science/ingredient/FAQ surfaces; `description_tag` leads with the receipt.

---

## 12. Open decisions you must honour (from FOUNDER-DECISIONS.md)

These were removed or restricted during the claims-grounding sweep. **Do not re-introduce them:**

- **Date (TA14):** "reduces breakage 50%" and "12× deeper / reaches the follicle" are **removed — unsourced.** Date's only receipt is **skin hydration** (Lestari 2024). Do not write a breakage or follicular-delivery claim.
- **Strawberry (TA05):** no peer-reviewed hair evidence. Hero lives on the **NMF/ferment/protein base**, not the berry. Strawberry = antioxidant/sensorial signature only.
- **Detox (TA08) salicylic acid 0.4%:** no anti-dandruff verb (see §4).
- **Ghassoul:** it's **lacustrine stevensite, not volcanic**; makes **no remineralization** claim. (And confirm it's actually in the BOM before citing its mechanism.)
- **Silverfree:** mechanism-only; **no reverse-grey claim** (§6).
- **Onion 87%:** alopecia-areata-specific; hair-density hero is **Capixyl**, not the 87% figure.
- **Argan, Black Garlic "10×", Niacinamide "keratin":** all corrected/removed — write from the corrected `ingredients-page-LIVE.md`, never from old PDP copy.

---

*Canonical sources: `C:\Users\narha\Dropbox\Tara\_proof-engine\` (claims, corpus, science-hub) · `C:\Users\narha\Dropbox\Tara\tara-formulator\` (the live fact-checker). When the data and old copy disagree, the Proof Engine wins. When in doubt, under-claim — restraint is the brand.*
