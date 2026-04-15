# CityLens — Project Log

## Phase 1: Data Engineering Pipeline

### Housing
- Ingested Boston Property Assessment FY2025 (183,445 rows)
- Built 10-layer pipeline: RAW → STAGING → CORE → MART → SERVING
- Resolved neighborhood mapping issues (READVILLE → HYDE PARK, etc.)
- Built 6 SERVING tables for AI retrieval

### Transportation
- Ingested MBTA data: 161.7M rows across HR/LR events, travel times, alerts, ridership, weather
- Built 6-layer pipeline with dimensional model
- Built anomaly detection using Z-score analysis
- Built weather impact analysis (NOAA data joined with route performance)
- Built 13 SERVING tables for AI retrieval
- Deployed Streamlit dashboard with 5 tabs

### Crime
- Ingested Boston Crime Incident Reports
- Built RAW → CLEAN pipeline
- Generated CRIME_SUMMARIES with vector embeddings
- Ingested policy documents with vector embeddings

---

## Phase 2: Database Migration

- Merged CITY_HOUSING_DB + CITY_LENS_DB into CITYLENS_MERGED_DB
- Updated all schema references across agents
- Warehouse renamed to EAGLE_WH

---

## Phase 3: AI Agent Development

### Housing Agent (Snowflake Notebook)
- Built Router with 7 intent types
- Built 6 analysts: neighborhood, price, property, zipcode, building_type, value_trend
- Integrated Snowflake Cortex (claude-haiku-4-5)
- Added Query Logger to HOUSING_MONITORING.QUERY_LOG
- Added Reflection scoring

### Transportation Agent (Snowflake Notebook)
- Built Router with 8 intent types
- Built 8 analysts: performance, weather, anomaly, alerts, travel_time, station, reliability, monthly
- Integrated Snowflake Cortex (claude-haiku-4-5)

### Crime Agent (Snowflake Notebook)
- Built Router with 6 intent types
- Built 6 analysts: offense, district, trend, shooting, time, policy
- Integrated Snowflake Cortex (claude-haiku-4-5)

---

## Phase 4: RAG / Embedding

- Added EMBEDDING column to HOUSING_SERVING.SRV_HOUSING_QA_CONTEXT (115 rows)
- Added EMBEDDING column to CITYLENS_SERVING.SRV_QA_CONTEXT (13,600 rows)
- Used snowflake-arctic-embed-m model
- Updated neighborhood_analyst and performance_analyst to use VECTOR_COSINE_SIMILARITY

---

## Phase 5: LangGraph Integration

- Built CityLens LangGraph pipeline with 4 nodes:
  - Router Node
  - Retrieval Node
  - Synthesis Node
  - Reflection Node
- Supports 4 branches: housing, transportation, crime, cross
- Connected to Snowflake from local Python environment

---

## Phase 6: Cross-Branch Analysis

- Built DISTRICT_NEIGHBORHOOD_MAP (police districts → neighborhoods)
- Built STATION_NEIGHBORHOOD_MAP (MBTA stations → neighborhoods)
- Updated cross_branch_analyst to join Housing + Crime + Transportation
- Key finding: Housing prices in Boston driven by prestige and location, not crime rates
