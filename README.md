# 🏙️ CityLens — Boston Urban Intelligence System

> A multi-agent AI system that answers questions about Boston's housing, transportation, and crime data using LangGraph, RAG, and Snowflake Cortex.

---

## 📌 Project Overview

CityLens is an end-to-end data engineering and AI agent system built on top of Boston's open city data. It enables natural language querying across three domains:

- 🏠 **Housing** — Property values, neighborhood tiers, price per sqft
- 🚇 **Transportation** — MBTA reliability, delays, weather impact, station activity
- 🚨 **Crime** — Incident patterns, shooting hotspots, district-level analysis
- 🔀 **Cross-branch** — Multi-domain analysis (e.g. "Does crime affect housing prices?")

---

## 🏗️ System Architecture

```
User Query
    ↓
[Router Node]         — Identifies branch (housing / transportation / crime / cross)
    ↓
[Retrieval Node]      — Pulls relevant data from Snowflake SERVING layer via RAG
    ↓
[Synthesis Node]      — Calls claude-haiku-4-5 via Snowflake Cortex to generate answer
    ↓
[Reflection Node]     — Scores answer quality
    ↓
Final Answer
```

**Technology Stack:**

| Component | Technology |
|---|---|
| Orchestration | LangGraph |
| Data Warehouse | Snowflake |
| LLM | Claude Haiku 4.5 (via Snowflake Cortex) |
| Embedding | Snowflake Arctic Embed M |
| Vector Search | Snowflake VECTOR_COSINE_SIMILARITY |
| Pipeline | SQL (multi-layer: RAW → STAGING → CORE → MART → SERVING) |

---

## 📊 Data Sources

| Branch | Source | Volume |
|---|---|---|
| Housing | Boston Property Assessment FY2025 | 183,445 properties |
| Transportation | MBTA Events, Travel Times, Alerts, Ridership, Weather | 161.7M rows |
| Crime | Boston Crime Incident Reports | 400,000+ incidents |

---

## 🔄 Data Flow

```
Raw Data (CSV / API)
    ↓
RAW Layer        — Original data, all VARCHAR
    ↓
STAGING Layer    — Type casting, cleaning, validation flags
    ↓
CORE Layer       — Dimensional model (Facts + Dimensions)
    ↓
MART Layer       — Business aggregations
    ↓
SERVING Layer    — AI-ready summaries + Vector Embeddings
    ↓
LangGraph Agent  — RAG retrieval + LLM generation
    ↓
Final Answer
```

---

## 🤖 Agent Architecture

### Branch Routing
The Router Node identifies which domain(s) the query belongs to:
- Keyword scoring across housing / transportation / crime domains
- Cross-branch detection for multi-domain questions
- Entity extraction (MBTA line, neighborhood name)

### RAG Pipeline
Each branch uses vector similarity search against pre-embedded SERVING tables:
```
User Query → EMBED_TEXT_768 → VECTOR_COSINE_SIMILARITY → Top-K Documents → LLM
```

### Analysts per Branch

**Housing (6 analysts):**
- `neighborhood_analyst` — Neighborhood tier and value scores
- `price_analyst` — Top/bottom price rankings
- `property_analyst` — Property-level summaries
- `zipcode_analyst` — Zipcode-level aggregations
- `building_type_analyst` — Condo vs single family comparison
- `value_trend_analyst` — Price distribution and variance

**Transportation (8 analysts):**
- `performance_analyst` — Route-level performance
- `reliability_analyst` — Reliability grades and bad days
- `anomaly_analyst` — Statistical anomaly detection (Z-score)
- `alerts_analyst` — Service alert summaries
- `weather_analyst` — Weather impact on performance
- `travel_time_analyst` — Best travel times by daypart
- `station_analyst` — Busiest stations and risk rankings
- `monthly_analyst` — Month-over-month trends

**Crime (6 analysts):**
- `offense_analyst` — Most common crime types
- `district_analyst` — District-level crime rankings
- `trend_analyst` — Monthly crime trends
- `shooting_analyst` — Shooting hotspot analysis
- `time_analyst` — Peak crime times by day/hour
- `policy_analyst` — Policy document retrieval

**Cross-branch:**
- Joins Housing + Crime + Transportation via geographic mapping tables
- `DISTRICT_NEIGHBORHOOD_MAP` — Links police districts to neighborhoods
- `STATION_NEIGHBORHOOD_MAP` — Links MBTA stations to neighborhoods

---

## 📈 Evaluation

| Metric | Method |
|---|---|
| Retrieval Quality | Vector cosine similarity scores |
| Answer Quality | LLM-as-judge reflection scoring |
| Latency | Measured per query (avg 4-7 seconds) |
| Groundedness | Score based on specificity, numbers, named entities |

---

## 🚀 Setup Instructions

### Prerequisites
- Python 3.10+
- Anaconda
- Snowflake account with access to `CITYLENS_MERGED_DB`

### Installation

```bash
# Create conda environment
conda create -n citylens python=3.10 -y
conda activate citylens

# Install dependencies
pip install langgraph langchain langchain-anthropic snowflake-snowpark-python pandas
pip install "snowflake-connector-python[pandas]"
```

### Configuration

```bash
# Copy the template and fill in your credentials
cp snowflake_config_template.py snowflake_config.py
```

Edit `snowflake_config.py`:
```python
SNOWFLAKE_CONN = {
    "account":   "PGB87192",
    "user":      "YOUR_USERNAME",
    "password":  "YOUR_PASSWORD",
    "warehouse": "EAGLE_WH",
    "database":  "CITYLENS_MERGED_DB",
    "role":      "TRAINING_ROLE",
}
```

### Run

```bash
python citylens_langgraph.py
```

---

## 💬 Example Questions

**Housing:**
- "What are the most expensive neighborhoods in Boston?"
- "What are the cheapest neighborhoods to buy?"
- "How do condos compare to single family homes?"

**Transportation:**
- "Which MBTA line is the most reliable?"
- "What is the best time to ride the Blue Line?"
- "Which routes have the most delays?"

**Crime:**
- "What are the most dangerous areas in Boston?"
- "How has crime trended over the past two years?"
- "Which districts have the most shootings?"

**Cross-branch:**
- "Where should I live in Boston?"
- "Does high pricing area have low crime rate?"
- "How does commute convenience impact housing value?"

---

## 🗂️ Repository Structure

```
citylens/
├── citylens_langgraph.py          ← Main LangGraph pipeline
├── snowflake_config_template.py   ← Connection config template
├── requirements.txt               ← Python dependencies
├── .gitignore
├── housing/
│   ├── housing_agent.py           ← Housing Snowflake Notebook agent
│   └── sql/                       ← Housing pipeline SQL files
├── transportation/
│   ├── transport_agent.py         ← Transportation Snowflake Notebook agent
│   └── sql/                       ← Transportation pipeline SQL files
├── crime/
│   ├── crime_agent.py             ← Crime Snowflake Notebook agent
│   └── sql/                       ← Crime pipeline SQL files
├── cross/
│   └── sql/
│       ├── district_neighborhood.sql
│       └── station_neighborhood.sql
└── docs/
    └── project_log.md             ← Project progress log
```

---

## 📝 Project Log

See [docs/project_log.md](docs/project_log.md) for detailed project progress.
