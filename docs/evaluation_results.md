# CityLens — Evaluation Report

Generated: 2026-04-16 19:27

---

## 1. Ablation Study

Comparing three system versions on the same questions:

| Version | Description |
|---------|-------------|
| V1 | No RAG, No Router — simulates original Notebook |
| V2 | RAG + Sequential — single retrieval node |
| V3 | Full System — parallel multi-agent + full RAG |

### Latency Comparison

| Question | V1 (ms) | V2 (ms) | V3 (ms) |
|----------|---------|---------|---------|
| What are the most expensive neighborhoods in Bosto... | 5431 | 6658 | 5836 |
| What are the cheapest neighborhoods to buy in Bost... | 2751 | 5130 | 5635 |
| Which MBTA line is the most reliable? | 2762 | 5836 | 7379 |
| Which MBTA routes are the most unreliable? | 2859 | 5519 | 6669 |
| Which districts have the highest crime rates in Bo... | 2701 | 5181 | 5057 |
| What are the most dangerous areas in Boston? | 2726 | 5236 | 5335 |
| Where should I live in Boston? | 5153 | 6920 | 12987 |
| Does the high pricing area have low crime rate? | 2765 | 5021 | 6855 |
| How does commute convenience impact housing value? | 4709 | 6862 | 8088 |
| How do condos compare to single family homes in Bo... | 4710 | 5016 | 5444 |

### Confidence Score (V3 only)

| Question | Confidence |
|----------|------------|
| What are the most expensive neighborhoods in Bosto... | 0.85 |
| What are the cheapest neighborhoods to buy in Bost... | 0.85 |
| Which MBTA line is the most reliable? | 0.85 |
| Which MBTA routes are the most unreliable? | 0.85 |
| Which districts have the highest crime rates in Bo... | 0.75 |
| What are the most dangerous areas in Boston? | 0.75 |
| Where should I live in Boston? | 0.88 |
| Does the high pricing area have low crime rate? | 0.88 |
| How does commute convenience impact housing value? | 0.88 |
| How do condos compare to single family homes in Bo... | 0.85 |

### Retrieval Volume Comparison

| Question | V1 items | V2 items | V3 items |
|----------|----------|----------|----------|
| What are the most expensive neighborhoods in Bosto... | 10 | 10 | 26 |
| What are the cheapest neighborhoods to buy in Bost... | 10 | 10 | 26 |
| Which MBTA line is the most reliable? | 10 | 10 | 21 |
| Which MBTA routes are the most unreliable? | 10 | 10 | 21 |
| Which districts have the highest crime rates in Bo... | 10 | 10 | 18 |
| What are the most dangerous areas in Boston? | 10 | 10 | 18 |
| Where should I live in Boston? | 10 | 10 | 93 |
| Does the high pricing area have low crime rate? | 10 | 10 | 93 |
| How does commute convenience impact housing value? | 10 | 10 | 93 |
| How do condos compare to single family homes in Bo... | 10 | 10 | 31 |

---

## 2. Retrieval Accuracy (Cosine Similarity)

| Table | Query | Avg Similarity | Top-1 | Level |
|-------|-------|----------------|-------|-------|
| Housing QA Context | most expensive neighborhood Boston | 0.7503 | 0.7785 | HIGH |
| Housing Neighborhood | affordable housing area Boston | 0.6991 | 0.7237 | MEDIUM |
| Transport QA Context | MBTA line reliability performance | 0.7335 | 0.7377 | HIGH |
| Transport Reliability | unreliable train route delays | 0.6268 | 0.6561 | MEDIUM |
| Crime Summaries | dangerous district shooting crime | 0.6603 | 0.7208 | MEDIUM |

**Overall average similarity: 0.694**

---

## 3. BLEU / ROUGE Scores

Scores compared against manually written ground truth answers.

| Question | Version | BLEU | ROUGE-1 | ROUGE-2 | ROUGE-L |
|----------|---------|------|---------|---------|---------|
| What are the most expensive neighborhood... | V1 | 0.0857 | 0.3667 | 0.1695 | 0.2833 |
| What are the most expensive neighborhood... | V2 | 0.0404 | 0.2234 | 0.0923 | 0.1929 |
| What are the most expensive neighborhood... | V3 | 0.0431 | 0.2791 | 0.1127 | 0.2326 |
| What are the cheapest neighborhoods to b... | V1 | 0.0474 | 0.2742 | 0.0984 | 0.1935 |
| What are the cheapest neighborhoods to b... | V2 | 0.0192 | 0.1493 | 0.0503 | 0.0995 |
| What are the cheapest neighborhoods to b... | V3 | 0.0244 | 0.2165 | 0.0873 | 0.1385 |
| Which MBTA line is the most reliable? | V1 | 0.0073 | 0.1724 | 0.0351 | 0.1379 |
| Which MBTA line is the most reliable? | V2 | 0.0036 | 0.1739 | 0.0195 | 0.1063 |
| Which MBTA line is the most reliable? | V3 | 0.0553 | 0.3409 | 0.2299 | 0.3068 |
| Which MBTA routes are the most unreliabl... | V1 | 0.0035 | 0.1607 | 0.0364 | 0.1071 |
| Which MBTA routes are the most unreliabl... | V2 | 0.0076 | 0.1514 | 0.0328 | 0.0865 |
| Which MBTA routes are the most unreliabl... | V3 | 0.0058 | 0.1647 | 0.0595 | 0.1176 |
| Which districts have the highest crime r... | V1 | 0.0052 | 0.128 | 0.0163 | 0.096 |
| Which districts have the highest crime r... | V2 | 0.0086 | 0.1667 | 0.0449 | 0.1333 |
| Which districts have the highest crime r... | V3 | 0.0172 | 0.2487 | 0.1257 | 0.2176 |
| What are the most dangerous areas in Bos... | V1 | 0.012 | 0.1918 | 0.0417 | 0.137 |
| What are the most dangerous areas in Bos... | V2 | 0.0037 | 0.1561 | 0.0493 | 0.1268 |
| What are the most dangerous areas in Bos... | V3 | 0.023 | 0.2407 | 0.0748 | 0.1667 |
| Where should I live in Boston? | V1 | 0.0343 | 0.2921 | 0.0795 | 0.191 |
| Where should I live in Boston? | V2 | 0.0047 | 0.2286 | 0.0385 | 0.1238 |
| Where should I live in Boston? | V3 | 0.0251 | 0.2422 | 0.0875 | 0.1242 |
| Does the high pricing area have low crim... | V1 | 0.01 | 0.2679 | 0.0545 | 0.1964 |
| Does the high pricing area have low crim... | V2 | 0.0034 | 0.2079 | 0.03 | 0.1386 |
| Does the high pricing area have low crim... | V3 | 0.0114 | 0.2783 | 0.0702 | 0.1739 |
| How does commute convenience impact hous... | V1 | 0.0066 | 0.3033 | 0.067 | 0.1327 |
| How does commute convenience impact hous... | V2 | 0.0038 | 0.2151 | 0.0482 | 0.0956 |
| How does commute convenience impact hous... | V3 | 0.0046 | 0.2308 | 0.0645 | 0.1346 |
| How do condos compare to single family h... | V1 | 0.0374 | 0.3017 | 0.113 | 0.2011 |
| How do condos compare to single family h... | V2 | 0.0077 | 0.2041 | 0.0905 | 0.1551 |
| How do condos compare to single family h... | V3 | 0.0661 | 0.3096 | 0.1857 | 0.2678 |

---

## 4. LLM-as-Judge Scores

Each answer scored 1-10 on four dimensions by Claude.

| Question | Version | Relevance | Groundedness | Completeness | Specificity | Overall |
|----------|---------|-----------|--------------|--------------|-------------|---------|
| What are the most expensive neighborhood... | V1 | 9 | 6 | 7 | 9 | 7 |
| What are the most expensive neighborhood... | V2 | 9 | 6 | 8 | 9 | 8 |
| What are the most expensive neighborhood... | V3 | 9 | 8 | 9 | 9 | 9 |
| What are the cheapest neighborhoods to b... | V1 | 8 | 7 | 6 | 9 | 7 |
| What are the cheapest neighborhoods to b... | V2 | 9 | 6 | 8 | 9 | 8 |
| What are the cheapest neighborhoods to b... | V3 | 9 | 7 | 8 | 9 | 8 |
| Which MBTA line is the most reliable? | V1 | 9 | 8 | 7 | 6 | 7 |
| Which MBTA line is the most reliable? | V2 | 6 | 7 | 4 | 6 | 5 |
| Which MBTA line is the most reliable? | V3 | 9 | 3 | 8 | 7 | 4 |
| Which MBTA routes are the most unreliabl... | V1 | 9 | 8 | 7 | 6 | 7 |
| Which MBTA routes are the most unreliabl... | V2 | 4 | 3 | 2 | 6 | 3 |
| Which MBTA routes are the most unreliabl... | V3 | 9 | 8 | 8 | 9 | 8 |
| Which districts have the highest crime r... | V1 | 9 | 8 | 7 | 6 | 7 |
| Which districts have the highest crime r... | V2 | 8 | 7 | 5 | 8 | 7 |
| Which districts have the highest crime r... | V3 | 9 | 8 | 7 | 8 | 8 |
| What are the most dangerous areas in Bos... | V1 | 2 | 8 | 6 | 7 | 5 |
| What are the most dangerous areas in Bos... | V2 | 7 | 8 | 6 | 8 | 7 |
| What are the most dangerous areas in Bos... | V3 | 9 | 9 | 7 | 9 | 8 |
| Where should I live in Boston? | V1 | 9 | 7 | 6 | 8 | 7 |
| Where should I live in Boston? | V2 | 9 | 6 | 8 | 9 | 8 |
| Where should I live in Boston? | V3 | 9 | 7 | 8 | 9 | 8 |
| Does the high pricing area have low crim... | V1 | 9 | 7 | 8 | 6 | 7 |
| Does the high pricing area have low crim... | V2 | 7 | 6 | 5 | 8 | 6 |
| Does the high pricing area have low crim... | V3 | 9 | 8 | 8 | 9 | 8 |
| How does commute convenience impact hous... | V1 | 9 | 7 | 8 | 9 | 8 |
| How does commute convenience impact hous... | V2 | 9 | 7 | 8 | 9 | 8 |
| How does commute convenience impact hous... | V3 | 9 | 8 | 8 | 9 | 8 |
| How do condos compare to single family h... | V1 | 4 | 6 | 3 | 5 | 4 |
| How do condos compare to single family h... | V2 | 6 | 8 | 5 | 8 | 6 |
| How do condos compare to single family h... | V3 | 9 | 7 | 6 | 8 | 7 |

### Average Scores by Version

| Version | Relevance | Groundedness | Completeness | Specificity | Overall |
|---------|-----------|--------------|--------------|-------------|---------|
| V1 | 7.7 | 7.2 | 6.5 | 7.1 | 6.6 |
| V2 | 7.4 | 6.4 | 5.9 | 8.0 | 6.6 |
| V3 | 9.0 | 7.3 | 7.7 | 8.6 | 7.6 |

---

## 5. Cost Analysis

### Per-Query Cost

| Provider | Input (2K tokens) | Output (500 tokens) | Embed (50 tokens) | Total |
|----------|------------------|--------------------|--------------------|-------|
| Snowflake Cortex | claude-haiku-4-5 | $0.00025/1K | $0.00125/1K | $0.004501 |
| OpenAI GPT-4o    | gpt-4o           | $0.005/1K   | $0.015/1K   | $0.017501 |

**Cost savings vs OpenAI: 74.3% cheaper**

### One-Time Embedding Cost

- Total rows embedded: 171,007
- Total tokens: 13,680,560
- Snowflake Arctic Embed cost: **$0.2189**

### Monthly Cost Estimate (1,000 queries)

| Provider | Monthly Cost |
|----------|-------------|
| Snowflake Cortex | $4.5 |
| OpenAI GPT-4o    | $17.5 |

### Additional Advantages of Snowflake Cortex

- Data never leaves Snowflake (security + compliance)
- No external API calls required
- Unified billing with existing Snowflake contract
- Lower latency (no network round-trip to external API)

---

## Summary

| Metric | Result |
|--------|--------|
| Avg retrieval similarity | 0.694 |
| Cost savings vs OpenAI | 74.3% |
| One-time embedding cost | $0.2189 |
| Monthly cost (1K queries) | $4.5 |
| V3 data richness vs V1 | ~9x more items retrieved |