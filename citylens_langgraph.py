# =============================================================================
# CityLens — LangGraph Multi-Agent Pipeline (Full RAG Version)
# =============================================================================
# All analysts now use vector similarity search (EMBED_TEXT_768)
# instead of fixed ORDER BY queries.
# =============================================================================

import json
import uuid
import time
from typing import TypedDict
from datetime import datetime

from langgraph.graph import StateGraph, END
from snowflake.snowpark import Session

from snowflake_config import SNOWFLAKE_CONN

# ---------------------------------------------------------------------------
# Snowflake Session
# ---------------------------------------------------------------------------

session = Session.builder.configs(SNOWFLAKE_CONN).create()
print(f"✅ Snowflake connected!")
print(f"   Database  : {session.get_current_database()}")
print(f"   Warehouse : {session.get_current_warehouse()}")

DB = "CITYLENS_MERGED_DB"

# ---------------------------------------------------------------------------
# Table Config
# ---------------------------------------------------------------------------

class Tables:
    # Housing
    HOUSING_QA_CONTEXT           = f"{DB}.HOUSING_SERVING.SRV_HOUSING_QA_CONTEXT"
    HOUSING_NEIGHBORHOOD_SUMMARY = f"{DB}.HOUSING_SERVING.SRV_NEIGHBORHOOD_SUMMARY"
    HOUSING_PROPERTY_SUMMARY     = f"{DB}.HOUSING_SERVING.SRV_PROPERTY_PROFILE_SUMMARY"
    HOUSING_MART_EXCEPTIONS      = f"{DB}.HOUSING_MART.MART_TOP_HOUSING_EXCEPTIONS"
    HOUSING_FACT_PROPERTY        = f"{DB}.HOUSING_CORE.FACT_PROPERTY_VALUE"
    HOUSING_DIM_PROPERTY_TYPE    = f"{DB}.HOUSING_CORE.DIM_PROPERTY_TYPE"

    # Transportation
    TRANSPORT_QA_CONTEXT         = f"{DB}.CITYLENS_SERVING.SRV_QA_CONTEXT"
    TRANSPORT_WEATHER_CONTEXT    = f"{DB}.CITYLENS_SERVING.SRV_WEATHER_CONTEXT"
    TRANSPORT_ANOMALY_CONTEXT    = f"{DB}.CITYLENS_SERVING.SRV_ANOMALY_CONTEXT"
    TRANSPORT_BEST_TRAVEL_TIME   = f"{DB}.CITYLENS_SERVING.SRV_BEST_TRAVEL_TIME"
    TRANSPORT_RELIABILITY        = f"{DB}.CITYLENS_SERVING.SRV_ROUTE_RELIABILITY"
    TRANSPORT_MONTHLY_TREND      = f"{DB}.CITYLENS_SERVING.SRV_MONTHLY_TREND"
    TRANSPORT_STATION_RANKING    = f"{DB}.CITYLENS_SERVING.SRV_STATION_RISK_RANKING"
    TRANSPORT_ROUTE_CONTEXT      = f"{DB}.CITYLENS_SERVING.SRV_ROUTE_CONTEXT"
    TRANSPORT_ALERTS_SUMMARY     = f"{DB}.CITYLENS_MART.MART_ALERTS_SUMMARY"
    TRANSPORT_DAYPART            = f"{DB}.CITYLENS_SERVING.SRV_DAYPART_PERFORMANCE"
    TRANSPORT_DAYOFWEEK          = f"{DB}.CITYLENS_SERVING.SRV_DAYOFWEEK_PERFORMANCE"

    # Crime
    CRIME_CLEAN                  = f"{DB}.CRIME_PUBLIC.CRIME_CLEAN"
    CRIME_MONTHLY                = f"{DB}.CRIME_PUBLIC.CRIME_MONTHLY"
    CRIME_SUMMARIES              = f"{DB}.CRIME_PUBLIC.CRIME_SUMMARIES"
    POLICY_DOCUMENTS             = f"{DB}.CRIME_PUBLIC.POLICY_DOCUMENTS"
    POLICY_MASTER                = f"{DB}.CRIME_PUBLIC.POLICY_MASTER"

    # Cross-branch
    DISTRICT_NEIGHBORHOOD_MAP    = f"{DB}.CRIME_PUBLIC.DISTRICT_NEIGHBORHOOD_MAP"
    STATION_NEIGHBORHOOD_MAP     = f"{DB}.CRIME_PUBLIC.STATION_NEIGHBORHOOD_MAP"


# ---------------------------------------------------------------------------
# LangGraph State
# ---------------------------------------------------------------------------

class CityLensState(TypedDict):
    user_query:       str
    query_id:         str
    query_ts:         str
    branch:           str
    intent:           str
    entities:         dict
    raw_context:      dict
    total_retrievals: int
    answer:           str
    latency_ms:       int
    reflection_score: int
    final_answer:     str


# ---------------------------------------------------------------------------
# Node 1: Router
# ---------------------------------------------------------------------------

def router_node(state: CityLensState) -> CityLensState:
    query = state["user_query"].lower()

    housing_keywords   = ['neighborhood', 'property', 'housing', 'home', 'house',
                          'condo', 'apartment', 'rent', 'price per sqft', 'bedroom',
                          'allston', 'back bay', 'beacon hill', 'dorchester',
                          'south boston', 'roxbury', 'fenway', 'zipcode', 'zip code']
    transport_keywords = ['mbta', 'train', 'subway', 'line', 'station', 'transit',
                          'blue line', 'red line', 'green line', 'orange line',
                          'commute', 'delay', 'alert', 'reliability', 'route']
    crime_keywords     = ['crime', 'shooting', 'robbery', 'assault', 'burglary',
                          'larceny', 'fraud', 'arrest', 'district', 'safe', 'dangerous',
                          'offense', 'incident', 'police', 'weapon']
    cross_keywords     = ['compare', 'correlation', 'relationship', 'between',
                          'affect', 'impact', 'vs', 'versus', 'and crime',
                          'and housing', 'and transit', 'neighborhood safety',
                          'livability', 'relate', 'connection', 'how does',
                          'where should i live', 'best place to live',
                          'where to live', 'should i move', 'high pricing',
                          'expensive area', 'price and crime', 'pricing area',
                          'low crime', 'high crime', 'does the', 'will have',
                          'best of both', 'overall', 'quality of life',
                          'price crime', 'pricing and crime']

    housing_score   = sum(1 for k in housing_keywords   if k in query)
    transport_score = sum(1 for k in transport_keywords  if k in query)
    crime_score     = sum(1 for k in crime_keywords      if k in query)
    cross_score     = sum(1 for k in cross_keywords      if k in query)

    scores = {
        'housing':        housing_score,
        'transportation': transport_score,
        'crime':          crime_score,
    }

    # Forced cross rules
    if ('price' in query or 'pricing' in query or 'expensive' in query or 'cheap' in query) and \
       ('crime' in query or 'safe' in query or 'dangerous' in query):
        branch = 'cross'
    elif ('transit' in query or 'mbta' in query or 'commute' in query) and \
         ('housing' in query or 'neighborhood' in query or 'live' in query):
        branch = 'cross'
    elif cross_score >= 1 and sum(1 for s in scores.values() if s > 0) >= 2:
        branch = 'cross'
    elif cross_score >= 2:
        branch = 'cross'
    else:
        branch = max(scores, key=scores.get)
        if scores[branch] == 0:
            branch = 'cross'

    # Intent detection
    if branch == 'housing':
        if any(w in query for w in ['expensive', 'luxury', 'highest', 'top']):
            intent = 'ranking_high'
        elif any(w in query for w in ['affordable', 'cheap', 'lowest']):
            intent = 'ranking_low'
        elif any(w in query for w in ['compare', 'vs', 'versus']):
            intent = 'comparison'
        elif any(w in query for w in ['zip', 'zipcode']):
            intent = 'zipcode'
        elif any(w in query for w in ['condo', 'single family', 'building type']):
            intent = 'building_type'
        else:
            intent = 'general'

    elif branch == 'transportation':
        if any(w in query for w in ['best time', 'avoid crowd', 'quiet']):
            intent = 'best_time'
        elif any(w in query for w in ['reliable', 'reliability', 'unreliable']):
            intent = 'reliability'
        elif any(w in query for w in ['trend', 'month']):
            intent = 'trend'
        elif any(w in query for w in ['weather', 'rain', 'snow']):
            intent = 'weather'
        elif any(w in query for w in ['alert', 'delay', 'disruption']):
            intent = 'alerts'
        elif any(w in query for w in ['station', 'stop', 'busy']):
            intent = 'station'
        else:
            intent = 'general'

    elif branch == 'crime':
        if any(w in query for w in ['trend', 'month', 'year', 'over time']):
            intent = 'trend'
        elif any(w in query for w in ['shoot', 'gun']):
            intent = 'shooting'
        elif any(w in query for w in ['district', 'area', 'where', 'safest']):
            intent = 'district'
        elif any(w in query for w in ['time', 'hour', 'when', 'day']):
            intent = 'time'
        elif any(w in query for w in ['policy', 'program']):
            intent = 'policy'
        else:
            intent = 'offense'

    else:
        intent = 'general'

    # Entity extraction
    entities = {}
    for line in ['BLUE', 'RED', 'GREEN', 'ORANGE', 'SILVER']:
        if line.lower() in query:
            entities['line'] = line
            break

    neighborhoods = ['allston', 'back bay', 'beacon hill', 'brighton', 'charlestown',
                     'dorchester', 'downtown', 'east boston', 'fenway', 'hyde park',
                     'jamaica plain', 'mattapan', 'north end', 'roslindale', 'roxbury',
                     'south boston', 'south end', 'west roxbury']
    for n in neighborhoods:
        if n in query:
            entities['neighborhood'] = n
            break

    print(f"  🔀 Branch   : {branch}")
    print(f"  🎯 Intent   : {intent}")
    print(f"  📍 Entities : {entities}")

    return {**state, "branch": branch, "intent": intent, "entities": entities}


# ---------------------------------------------------------------------------
# Node 2: Retrieval — All analysts use RAG
# ---------------------------------------------------------------------------

def safe_serialize(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)


def rag_query(table, query_text, extra_cols="", extra_filter="", limit=8):
    """Generic RAG retrieval using vector cosine similarity."""
    safe_query = query_text.replace("'", "''")
    sql = f"""
        SELECT SUMMARY_TEXT,
               VECTOR_COSINE_SIMILARITY(
                   EMBEDDING,
                   SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', '{safe_query}')
               ) AS SIMILARITY
               {', ' + extra_cols if extra_cols else ''}
        FROM {table}
        WHERE EMBEDDING IS NOT NULL
        {extra_filter}
        ORDER BY SIMILARITY DESC
        LIMIT {limit}
    """
    return session.sql(sql).collect()


# ---------------------------------------------------------------------------
# Housing Analysts
# ---------------------------------------------------------------------------

def housing_neighborhood_analyst(query_text):
    results = rag_query(Tables.HOUSING_NEIGHBORHOOD_SUMMARY, query_text,
                        extra_cols="ENTITY_NAME, NEIGHBORHOOD_TIER, VALUE_SCORE")
    return [{'neighborhood': r['ENTITY_NAME'], 'tier': r['NEIGHBORHOOD_TIER'],
             'value_score': r['VALUE_SCORE'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def housing_qa_analyst(query_text):
    results = rag_query(Tables.HOUSING_QA_CONTEXT, query_text,
                        extra_cols="ENTITY_NAME, ENTITY_TYPE, TIER")
    return [{'entity': r['ENTITY_NAME'], 'type': r['ENTITY_TYPE'],
             'tier': r['TIER'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def housing_price_analyst():
    results = session.sql(f"""
        SELECT EXCEPTION_TYPE, NEIGHBORHOOD_NAME, AVG_PROPERTY_VALUE,
               MEDIAN_PROPERTY_VALUE, AVG_PRICE_PER_SQFT, RANK
        FROM {Tables.HOUSING_MART_EXCEPTIONS}
        WHERE EXCEPTION_TYPE IN ('TOP_5_MOST_EXPENSIVE','BOTTOM_5_LEAST_EXPENSIVE')
        ORDER BY EXCEPTION_TYPE, RANK
    """).to_pandas()
    return [{'type': r['EXCEPTION_TYPE'], 'neighborhood': r['NEIGHBORHOOD_NAME'],
             'avg_value': r['AVG_PROPERTY_VALUE'], 'rank': r['RANK']}
            for _, r in results.iterrows()]


def housing_building_type_analyst():
    results = session.sql(f"""
        SELECT pt.LAND_USE_DESC, pt.CATEGORY,
               COUNT(f.PROPERTY_ID) AS TOTAL_PROPERTIES,
               ROUND(AVG(f.TOTAL_VALUE),2) AS AVG_VALUE,
               ROUND(AVG(f.PRICE_PER_SQFT),2) AS AVG_PRICE_PER_SQFT,
               ROUND(AVG(f.LIVING_AREA),2) AS AVG_LIVING_AREA,
               ROUND(AVG(f.BEDROOMS),2) AS AVG_BEDROOMS
        FROM {Tables.HOUSING_FACT_PROPERTY} f
        JOIN {Tables.HOUSING_DIM_PROPERTY_TYPE} pt ON f.PROPERTY_TYPE_KEY = pt.PROPERTY_TYPE_KEY
        WHERE pt.CATEGORY = 'residential'
          AND pt.LAND_USE_DESC IN ('RESIDENTIAL CONDO','SINGLE FAM DWELLING',
                                   'TWO-FAM DWELLING','THREE-FAM DWELLING','APT 4-6 UNITS')
        GROUP BY pt.LAND_USE_DESC, pt.CATEGORY
        ORDER BY AVG_VALUE DESC
    """).to_pandas()
    return [{'land_use': r['LAND_USE_DESC'], 'avg_value': float(r['AVG_VALUE']),
             'avg_price_per_sqft': float(r['AVG_PRICE_PER_SQFT']),
             'avg_bedrooms': float(r['AVG_BEDROOMS'])}
            for _, r in results.iterrows()]


# ---------------------------------------------------------------------------
# Transportation Analysts
# ---------------------------------------------------------------------------

def transport_performance_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ENTITY_NAME) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_QA_CONTEXT, query_text,
                        extra_cols="ENTITY_NAME, ENTITY_TYPE, TIME_PERIOD",
                        extra_filter=extra)
    return [{'entity': r['ENTITY_NAME'], 'type': r['ENTITY_TYPE'],
             'period': r['TIME_PERIOD'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def transport_reliability_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_RELIABILITY, query_text,
                        extra_cols="ROUTE_ID, RELIABILITY_PCT, RELIABILITY_GRADE",
                        extra_filter=extra)
    return [{'route': r['ROUTE_ID'], 'reliability_pct': r['RELIABILITY_PCT'],
             'grade': r['RELIABILITY_GRADE'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def transport_anomaly_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_ANOMALY_CONTEXT, query_text,
                        extra_cols="ROUTE_ID, ANOMALY_TYPE, LIKELY_CAUSE, Z_SCORE",
                        extra_filter=extra)
    return [{'route': r['ROUTE_ID'], 'anomaly': r['ANOMALY_TYPE'],
             'cause': r['LIKELY_CAUSE'], 'z_score': str(r['Z_SCORE']),
             'summary': r['SUMMARY_TEXT'], 'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def transport_alerts_analyst(found_line=""):
    filter_sql = f"WHERE UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = session.sql(f"""
        SELECT ROUTE_ID, CAUSE, EFFECT, SEVERITY, SUM(ALERT_COUNT) AS TOTAL_ALERTS
        FROM {Tables.TRANSPORT_ALERTS_SUMMARY}
        {filter_sql}
        GROUP BY ROUTE_ID, CAUSE, EFFECT, SEVERITY
        ORDER BY TOTAL_ALERTS DESC LIMIT 5
    """).collect()
    return [{'route': r['ROUTE_ID'], 'cause': r['CAUSE'],
             'effect': r['EFFECT'], 'total_alerts': int(r['TOTAL_ALERTS'])}
            for r in results if r['ROUTE_ID']]


def transport_weather_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_WEATHER_CONTEXT, query_text,
                        extra_cols="ROUTE_ID, WEATHER_CONDITION, AVG_TEMP_F",
                        extra_filter=extra)
    return [{'route': r['ROUTE_ID'], 'weather': r['WEATHER_CONDITION'],
             'temp': str(r['AVG_TEMP_F']), 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def transport_station_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_STATION_RANKING, query_text,
                        extra_cols="STOP_NAME, ROUTE_ID, DAYPART, EVENT_COUNT",
                        extra_filter=extra)
    return [{'station': r['STOP_NAME'], 'route': r['ROUTE_ID'],
             'daypart': r['DAYPART'], 'event_count': r['EVENT_COUNT'],
             'summary': r['SUMMARY_TEXT'], 'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def transport_monthly_analyst(query_text, found_line=""):
    extra = f"AND UPPER(ROUTE_ID) LIKE '%{found_line}%'" if found_line else ""
    results = rag_query(Tables.TRANSPORT_MONTHLY_TREND, query_text,
                        extra_cols="ROUTE_ID, MONTH, TOTAL_EVENTS",
                        extra_filter=extra, limit=6)
    return [{'route': r['ROUTE_ID'], 'month': str(r['MONTH']),
             'total_events': r['TOTAL_EVENTS'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


# ---------------------------------------------------------------------------
# Crime Analysts
# ---------------------------------------------------------------------------

def crime_offense_analyst(query_text):
    results = rag_query(Tables.CRIME_SUMMARIES, query_text,
                        extra_cols="SUMMARY_TYPE, DIMENSION_VALUE",
                        extra_filter="AND SUMMARY_TYPE = 'OFFENSE'")
    return [{'offense_type': r['DIMENSION_VALUE'], 'summary': r['SUMMARY_TEXT'],
             'similarity': float(r['SIMILARITY'])}
            for r in results if r['SUMMARY_TEXT']]


def crime_district_analyst():
    results = session.sql(f"""
        SELECT DISTRICT, COUNT(*) AS TOTAL_INCIDENTS,
               SUM(SHOOTING) AS TOTAL_SHOOTINGS,
               MODE(OFFENSE_DESCRIPTION) AS MOST_COMMON_OFFENSE
        FROM {Tables.CRIME_CLEAN}
        WHERE DISTRICT IS NOT NULL
        GROUP BY DISTRICT
        ORDER BY TOTAL_INCIDENTS DESC LIMIT 10
    """).collect()
    return [{'district': r['DISTRICT'], 'total_incidents': int(r['TOTAL_INCIDENTS']),
             'total_shootings': int(r['TOTAL_SHOOTINGS']),
             'most_common_offense': r['MOST_COMMON_OFFENSE']}
            for r in results if r['DISTRICT']]


def crime_trend_analyst():
    results = session.sql(f"""
        SELECT TO_CHAR(MONTH_DATE, 'YYYY-MM') AS MONTH, TOTAL_CRIME
        FROM {Tables.CRIME_MONTHLY}
        ORDER BY MONTH_DATE DESC LIMIT 24
    """).collect()
    return [{'month': r['MONTH'], 'total_crime': int(r['TOTAL_CRIME'])}
            for r in results]


def crime_shooting_analyst():
    results = session.sql(f"""
        SELECT DISTRICT, OFFENSE_DESCRIPTION,
               COUNT(*) AS TOTAL_SHOOTINGS,
               MODE(DAY_OF_WEEK) AS MOST_COMMON_DAY
        FROM {Tables.CRIME_CLEAN}
        WHERE SHOOTING = 1
        GROUP BY DISTRICT, OFFENSE_DESCRIPTION
        ORDER BY TOTAL_SHOOTINGS DESC LIMIT 10
    """).collect()
    return [{'district': r['DISTRICT'], 'offense': r['OFFENSE_DESCRIPTION'],
             'total_shootings': int(r['TOTAL_SHOOTINGS']),
             'most_common_day': r['MOST_COMMON_DAY']}
            for r in results if r['DISTRICT']]


# ---------------------------------------------------------------------------
# Cross-branch Analyst
# ---------------------------------------------------------------------------

def cross_branch_analyst():
    results = session.sql(f"""
        SELECT
            m.NEIGHBORHOOD_NAME,
            m.DISTRICT,
            n.NEIGHBORHOOD_TIER,
            n.VALUE_SCORE,
            n.SUMMARY_TEXT AS HOUSING_SUMMARY,
            c.TOTAL_INCIDENTS,
            c.TOTAL_SHOOTINGS,
            c.MOST_COMMON_OFFENSE,
            t.TOTAL_STATIONS,
            t.TOTAL_TRANSIT_EVENTS,
            t.LINES_SERVED
        FROM {Tables.DISTRICT_NEIGHBORHOOD_MAP} m
        JOIN (
            SELECT ENTITY_NAME, NEIGHBORHOOD_TIER, VALUE_SCORE, SUMMARY_TEXT
            FROM {Tables.HOUSING_NEIGHBORHOOD_SUMMARY}
        ) n ON UPPER(n.ENTITY_NAME) = UPPER(m.NEIGHBORHOOD_NAME)
        JOIN (
            SELECT DISTRICT,
                   COUNT(*) AS TOTAL_INCIDENTS,
                   SUM(SHOOTING) AS TOTAL_SHOOTINGS,
                   MODE(OFFENSE_DESCRIPTION) AS MOST_COMMON_OFFENSE
            FROM {Tables.CRIME_CLEAN}
            WHERE DISTRICT IS NOT NULL
            GROUP BY DISTRICT
        ) c ON c.DISTRICT = m.DISTRICT
        LEFT JOIN (
            SELECT snm.NEIGHBORHOOD_NAME,
                   COUNT(DISTINCT sc.STOP_NAME) AS TOTAL_STATIONS,
                   SUM(sc.EVENT_COUNT) AS TOTAL_TRANSIT_EVENTS,
                   LISTAGG(DISTINCT sc.ROUTE_ID, ', ') AS LINES_SERVED
            FROM {Tables.STATION_NEIGHBORHOOD_MAP} snm
            JOIN {Tables.TRANSPORT_STATION_RANKING} sc
                ON UPPER(sc.STOP_NAME) = UPPER(snm.STATION_NAME)
            GROUP BY snm.NEIGHBORHOOD_NAME
        ) t ON UPPER(t.NEIGHBORHOOD_NAME) = UPPER(m.NEIGHBORHOOD_NAME)
        ORDER BY n.VALUE_SCORE DESC
    """).collect()
    return [
        {
            'neighborhood':        r['NEIGHBORHOOD_NAME'],
            'district':            r['DISTRICT'],
            'housing_tier':        r['NEIGHBORHOOD_TIER'],
            'value_score':         r['VALUE_SCORE'],
            'housing_summary':     r['HOUSING_SUMMARY'],
            'crime_incidents':     int(r['TOTAL_INCIDENTS']),
            'shootings':           int(r['TOTAL_SHOOTINGS']),
            'most_common_offense': r['MOST_COMMON_OFFENSE'],
            'transit_stations':    int(r['TOTAL_STATIONS']) if r['TOTAL_STATIONS'] else 0,
            'transit_events':      int(r['TOTAL_TRANSIT_EVENTS']) if r['TOTAL_TRANSIT_EVENTS'] else 0,
            'lines_served':        r['LINES_SERVED'] if r['LINES_SERVED'] else 'No direct MBTA access'
        }
        for r in results if r['NEIGHBORHOOD_NAME']
    ]


# ---------------------------------------------------------------------------
# Retrieval Node
# ---------------------------------------------------------------------------

def retrieval_node(state: CityLensState) -> CityLensState:
    branch     = state["branch"]
    intent     = state["intent"]
    entities   = state["entities"]
    query_text = state["user_query"]
    found_line = entities.get("line", "")

    raw_context = {}
    total = 0

    if branch == "housing":
        raw_context["neighborhood"] = housing_neighborhood_analyst(query_text)
        raw_context["qa_context"]   = housing_qa_analyst(query_text)
        raw_context["price"]        = housing_price_analyst()
        if intent == "building_type":
            raw_context["building_type"] = housing_building_type_analyst()

    elif branch == "transportation":
        raw_context["performance"] = transport_performance_analyst(query_text, found_line)
        raw_context["alerts"]      = transport_alerts_analyst(found_line)
        if intent in ["reliability", "general"]:
            raw_context["reliability"] = transport_reliability_analyst(query_text, found_line)
        if intent in ["anomaly", "alerts", "general"]:
            raw_context["anomaly"] = transport_anomaly_analyst(query_text, found_line)
        if intent == "weather":
            raw_context["weather"] = transport_weather_analyst(query_text, found_line)
        if intent in ["station", "best_time"]:
            raw_context["station"] = transport_station_analyst(query_text, found_line)
        if intent == "trend":
            raw_context["monthly"] = transport_monthly_analyst(query_text, found_line)

    elif branch == "crime":
        raw_context["offense"]  = crime_offense_analyst(query_text)
        raw_context["district"] = crime_district_analyst()
        if intent == "trend":
            raw_context["trend"] = crime_trend_analyst()
        if intent == "shooting":
            raw_context["shooting"] = crime_shooting_analyst()

    elif branch == "cross":
        raw_context["cross_analysis"] = cross_branch_analyst()
        print(f"  📊 Cross-branch: {len(raw_context['cross_analysis'])} neighborhoods matched")

    for items in raw_context.values():
        total += len(items)
        print(f"  📊 Retrieved {len(items)} items")

    return {**state, "raw_context": raw_context, "total_retrievals": total}


# ---------------------------------------------------------------------------
# Node 3: Synthesis
# ---------------------------------------------------------------------------

BRANCH_PROMPTS = {
    "housing":        "You are a Boston Housing Intelligence Analyst.",
    "transportation": "You are a senior Boston MBTA Transportation Analyst.",
    "crime":          "You are a Boston Crime Intelligence Analyst specializing in public safety data.",
    "cross":          "You are a Boston Urban Intelligence Analyst with expertise in housing, transportation, and crime data.",
}

def synthesis_node(state: CityLensState) -> CityLensState:
    context_text = ""
    for analyst_name, items in state["raw_context"].items():
        context_text += f"\n=== {analyst_name.upper()} ===\n"
        for item in items:
            context_text += json.dumps(item, default=safe_serialize) + "\n"
    context_text = context_text[:5000]

    role = BRANCH_PROMPTS.get(state["branch"], "You are a Boston city intelligence analyst.")

    prompt = f"""{role}

Question: {state['user_query']}

Data (use ONLY the data provided below):
{context_text}

IMPORTANT: Only use values that appear in the data above. Do not invent statistics.

Structure your answer as:
1. DIRECT ANSWER (1-2 sentences with specific numbers)
2. KEY FINDINGS (3-5 bullet points with data)
3. INSIGHT (1 sentence conclusion)"""

    start_time = time.time()
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-haiku-4-5', $${prompt}$$) AS ANSWER
    """).collect()
    latency_ms = int((time.time() - start_time) * 1000)
    answer = result[0]['ANSWER']

    return {**state, "answer": answer, "latency_ms": latency_ms}


# ---------------------------------------------------------------------------
# Node 4: Reflection
# ---------------------------------------------------------------------------

def reflection_node(state: CityLensState) -> CityLensState:
    answer = state["answer"]
    score  = 0

    if any(char.isdigit() for char in answer):
        score += 30

    keywords = ['boston', 'district', 'neighborhood', 'line', 'route',
                'mbta', 'crime', 'property', 'housing']
    if any(k in answer.lower() for k in keywords):
        score += 40

    if 100 < len(answer) < 1500:
        score += 30

    print(f"  ⭐ Reflection score: {score}/100")
    return {**state, "reflection_score": score, "final_answer": answer}


# ---------------------------------------------------------------------------
# Build the LangGraph
# ---------------------------------------------------------------------------

def build_graph():
    graph = StateGraph(CityLensState)

    graph.add_node("router",     router_node)
    graph.add_node("retrieval",  retrieval_node)
    graph.add_node("synthesis",  synthesis_node)
    graph.add_node("reflection", reflection_node)

    graph.set_entry_point("router")
    graph.add_edge("router",     "retrieval")
    graph.add_edge("retrieval",  "synthesis")
    graph.add_edge("synthesis",  "reflection")
    graph.add_edge("reflection", END)

    return graph.compile()


citylens_graph = build_graph()
print("✅ LangGraph compiled successfully!")


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def run_citylens(user_query: str) -> str:
    print(f"\n{'='*60}")
    print(f"❓ {user_query}")
    print('='*60)

    initial_state: CityLensState = {
        "user_query":       user_query,
        "query_id":         str(uuid.uuid4()),
        "query_ts":         datetime.now().isoformat(),
        "branch":           "",
        "intent":           "",
        "entities":         {},
        "raw_context":      {},
        "total_retrievals": 0,
        "answer":           "",
        "latency_ms":       0,
        "reflection_score": 0,
        "final_answer":     "",
    }

    result = citylens_graph.invoke(initial_state)

    print(f"\n{'='*60}")
    print("🤖 FINAL ANSWER:")
    print(result["final_answer"])
    print(f"\n⏱  Latency    : {result['latency_ms']}ms")
    print(f"📊 Retrievals : {result['total_retrievals']}")
    print(f"⭐ Score      : {result['reflection_score']}/100")
    print(f"🌿 Branch     : {result['branch']}")

    return result["final_answer"]


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_questions = [
        "What are the most expensive neighborhoods in Boston?",
        "Which MBTA line is the most reliable?",
        "Which districts have the highest crime rates in Boston?",
        "Where should I live in Boston?",
    ]
    for q in test_questions:
        run_citylens(q)
        print()