# =============================================================================
# CityLens — LangGraph Parallel Multi-Agent Pipeline
# =============================================================================
# Architecture:
#
#   User Query
#       ↓
#   [Router Node]          — keyword detection → branch + intent + entities
#       ↓ Send() API
#   [Housing Agent]  ──┐
#   [Transport Agent]──┼→  [Aggregator Node] → [Synthesis Node] → [Reflection Node]
#   [Crime Agent]    ──┘
#
# For single-branch queries, only the relevant agent runs.
# For cross-branch queries, all three agents run in parallel.
# =============================================================================

import json
import uuid
import time
import operator
from typing import TypedDict, Annotated
from datetime import datetime

from langgraph.graph import StateGraph, END
from langgraph.types import Send
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


# Simple in-memory cache
_query_cache = {}
_conversation_history = []
CACHE_MAX_SIZE = 100
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
# agent_results uses Annotated[list, operator.add] so that multiple parallel
# agents can each append their results without overwriting each other.
# operator.add merges all lists: [housing_result] + [transport_result] + [crime_result]
# ---------------------------------------------------------------------------

class CityLensState(TypedDict):
    user_query:       str
    query_id:         str
    query_ts:         str
    branch:           str
    intent:           str
    entities:         dict
    agent_results:    Annotated[list, operator.add]  # parallel agents write here
    raw_context:      dict                           # aggregator consolidates here
    total_retrievals: int
    answer:           str
    latency_ms:       int
    reflection_score: int
    final_answer:     str
    confidence_score: float
    sub_questions:    list
    use_multistep:    bool


# ---------------------------------------------------------------------------
# Keyword Lists (used by Router Node)
# ---------------------------------------------------------------------------
# These keyword lists determine which branch a query belongs to.
# Each word/phrase is checked with `if keyword in query_lower`.

HOUSING_KEYWORDS = [
    # Property types
    'neighborhood', 'property', 'housing', 'home', 'house',
    'condo', 'apartment', 'rent', 'bedroom', 'sqft', 'price per sqft',
    # Boston neighborhoods
    'allston', 'back bay', 'beacon hill', 'brighton', 'charlestown',
    'chinatown', 'dorchester', 'downtown', 'east boston', 'fenway',
    'hyde park', 'jamaica plain', 'longwood', 'mattapan', 'mission hill',
    'north end', 'roslindale', 'roxbury', 'south boston', 'south end',
    'west end', 'west roxbury', 'waterfront',
    # Price-related
    'zipcode', 'zip code', 'affordable', 'expensive', 'luxury',
    'property value', 'home value', 'real estate', 'buy a home',
]

TRANSPORT_KEYWORDS = [
    # System names
    'mbta', 'train', 'subway', 'transit', 'commute',
    # Line names
    'blue line', 'red line', 'green line', 'orange line', 'silver line',
    'blue', 'red line', 'green', 'orange', 'silver',
    # Concepts
    'station', 'stop', 'route', 'line', 'bus', 'rail',
    'delay', 'alert', 'reliability', 'on time', 'schedule',
    'crowded', 'rush hour', 'commuter', 'ridership',
    'travel time', 'headway', 'frequency',
]

CRIME_KEYWORDS = [
    # Crime types
    'crime', 'shooting', 'robbery', 'assault', 'burglary',
    'larceny', 'fraud', 'vandalism', 'trespassing', 'theft',
    # Concepts
    'arrest', 'offense', 'incident', 'police', 'weapon',
    'dangerous', 'violence', 'homicide', 'drug',
    # Safety
    'safe', 'safety', 'unsafe', 'risk',
    # Geography
    'district', 'b2', 'b3', 'c11', 'd4', 'a1', 'a7',
]

CROSS_KEYWORDS = [
    # Comparison/relationship
    'compare', 'correlation', 'relationship', 'between',
    'affect', 'impact', 'vs', 'versus', 'connection',
    'link', 'relate', 'associated', 'influence', 'effect',
    # Combined domains
    'and crime', 'and housing', 'and transit',
    'crime and', 'housing and', 'transit and',
    # Living recommendations
    'where should i live', 'best place to live', 'where to live',
    'should i move', 'best neighborhood', 'recommend', 'ideal area',
    'good place to live', 'where to buy',
    # Price + safety combos
    'high pricing', 'expensive area', 'price and crime',
    'affordable and safe', 'cheap and safe', 'value and safety',
    'does price', 'how does price', 'does the', 'will have',
    'low crime', 'high crime', 'price crime',
    # Quality of life
    'livability', 'quality of life', 'best of both',
    'overall', 'trade off', 'pros and cons',
    # Transit + housing combos
    'near transit', 'good commute', 'transit access',
    'commute and housing', 'commute and neighborhood',
]

# Intent keywords per branch
HOUSING_INTENT_KEYWORDS = {
    'ranking_high':  ['expensive', 'luxury', 'highest', 'top', 'most valuable', 'priciest'],
    'ranking_low':   ['affordable', 'cheap', 'cheapest', 'lowest', 'least expensive', 'budget'],
    'comparison':    ['compare', 'vs', 'versus', 'difference', 'between'],
    'zipcode':       ['zip', 'zipcode', 'zip code'],
    'building_type': ['condo', 'single family', 'building type', 'apartment', 'two family'],
}

TRANSPORT_INTENT_KEYWORDS = {
    'best_time':   ['best time', 'avoid crowd', 'quiet', 'when to ride', 'least busy'],
    'reliability': ['reliable', 'reliability', 'unreliable', 'on time', 'consistent'],
    'trend':       ['trend', 'month', 'over time', 'monthly', 'trending'],
    'weather':     ['weather', 'rain', 'snow', 'storm', 'cold'],
    'alerts':      ['alert', 'delay', 'disruption', 'issue', 'problem', 'service change'],
    'station':     ['station', 'stop', 'busy', 'crowded', 'platform'],
}

CRIME_INTENT_KEYWORDS = {
    'trend':    ['trend', 'month', 'year', 'over time', 'increase', 'decrease'],
    'shooting': ['shoot', 'gun', 'firearm', 'weapon', 'shooting'],
    'district': ['district', 'area', 'where', 'safest', 'most dangerous', 'location'],
    'time':     ['time', 'hour', 'when', 'day', 'night', 'morning', 'peak'],
    'policy':   ['policy', 'program', 'strategy', 'initiative', 'law'],
}


# ---------------------------------------------------------------------------
# Node 1: Router
# ---------------------------------------------------------------------------

def router_node(state: CityLensState) -> CityLensState:
    """
    Step 1: Detect branch using keyword scoring.
    Step 2: Detect intent within branch.
    Step 3: Extract named entities (MBTA line, neighborhood).
    Step 4: Update state (branch/intent/entities).
           The actual Send() dispatch happens in route_to_agents().
    """
    # 如果有对话历史，把上下文融入 query 用于 branch 检测
    if state.get("conversation_history"):
        recent = state["conversation_history"][-2:]
        history_text = " ".join([h["content"] for h in recent])
        query = (state["user_query"] + " " + history_text).lower()
    else:
        query = state["user_query"].lower()


    # --- Branch detection via keyword scoring ---
    housing_score   = sum(1 for k in HOUSING_KEYWORDS   if k in query)
    transport_score = sum(1 for k in TRANSPORT_KEYWORDS  if k in query)
    crime_score     = sum(1 for k in CRIME_KEYWORDS      if k in query)
    cross_score     = sum(1 for k in CROSS_KEYWORDS      if k in query)

    scores = {
        'housing':        housing_score,
        'transportation': transport_score,
        'crime':          crime_score,
    }

    # Forced cross rules (explicit domain combinations)
    if ('price' in query or 'pricing' in query or 'expensive' in query or 'cheap' in query) and \
       ('crime' in query or 'safe' in query or 'dangerous' in query):
        branch = 'cross'
    elif ('transit' in query or 'mbta' in query or 'commute' in query) and \
         ('housing' in query or 'neighborhood' in query or 'live' in query):
        branch = 'cross'
    elif ('neighborhood' in query or 'area' in query) and \
         ('crime' in query or 'crime rate' in query or 'dangerous' in query or 'safe' in query):
        branch = 'cross'
    elif any(w in query for w in ['commercial', 'investment', 'office', 'retail', 'business']):
        branch = 'cross'  # 新增
    elif cross_score >= 1 and sum(1 for s in scores.values() if s > 0) >= 2:
        branch = 'cross'
    elif cross_score >= 2:
        branch = 'cross'
    else:
        branch = max(scores, key=scores.get)
        if scores[branch] == 0:
            branch = 'cross'  # fallback: unknown queries get all data

    # --- Intent detection ---
    if branch == 'housing':
        intent = 'general'
        for intent_name, keywords in HOUSING_INTENT_KEYWORDS.items():
            if any(k in query for k in keywords):
                intent = intent_name
                break

    elif branch == 'transportation':
        intent = 'general'
        for intent_name, keywords in TRANSPORT_INTENT_KEYWORDS.items():
            if any(k in query for k in keywords):
                intent = intent_name
                break

    elif branch == 'crime':
        intent = 'offense'
        for intent_name, keywords in CRIME_INTENT_KEYWORDS.items():
            if any(k in query for k in keywords):
                intent = intent_name
                break

    else:
        intent = 'general'

    # --- Entity extraction ---
    entities = {}

    # MBTA line detection
    for line in ['BLUE', 'RED', 'GREEN', 'ORANGE', 'SILVER']:
        if line.lower() in query:
            entities['line'] = line
            break

    # Neighborhood detection
    neighborhoods = [
        'allston', 'back bay', 'beacon hill', 'brighton', 'charlestown',
        'chinatown', 'dorchester', 'downtown', 'east boston', 'fenway',
        'hyde park', 'jamaica plain', 'mattapan', 'north end', 'roslindale',
        'roxbury', 'south boston', 'south end', 'west end', 'west roxbury'
    ]
    for n in neighborhoods:
        if n in query:
            entities['neighborhood'] = n
            break

    print(f"  🔀 Branch   : {branch}")
    print(f"  🎯 Intent   : {intent}")
    print(f"  📍 Entities : {entities}")
    print(f"  📊 Scores   : housing={housing_score} transport={transport_score} crime={crime_score} cross={cross_score}")

    return {**state, "branch": branch, "intent": intent, "entities": entities,
            "agent_results": []}


# ---------------------------------------------------------------------------
# route_to_agents: decides which agents to Send() based on branch
# ---------------------------------------------------------------------------

COMPLEX_PATTERNS = [
    'where should i live', 'is it a good place', 'should i buy',
    'best neighborhood for', 'worth living', 'good investment',
    'recommend', 'suggest', 'advise',
]

def decompose_node(state: CityLensState) -> CityLensState:
    query = state["user_query"]
    query_lower = query.lower()
    
    is_complex = any(p in query_lower for p in COMPLEX_PATTERNS) or \
                 (len(query.split()) > 10 and state["branch"] == "cross")
    
    if not is_complex:
        return {**state, "sub_questions": [], "use_multistep": False}

    decompose_prompt = f"""Break this question into 2-3 specific sub-questions.

Original: {query}

Respond in EXACT format:
SUB1: [question about housing prices or neighborhoods]
SUB2: [question about crime or safety]
SUB3: [question about transit or commute]"""

    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-haiku-4-5', $${decompose_prompt}$$) AS D
    """).collect()

    sub_questions = []
    for line in result[0]['D'].strip().split('\n'):
        if line.startswith('SUB') and ':' in line:
            sub_questions.append(line.split(':', 1)[1].strip())

    if sub_questions:
        print(f"  🔍 Multi-step: decomposed into {len(sub_questions)} sub-questions")
        for i, sq in enumerate(sub_questions):
            print(f"     {i+1}. {sq}")

    return {**state, "sub_questions": sub_questions, "use_multistep": len(sub_questions) > 0}

def route_to_agents(state: CityLensState):
    """
    Called as a conditional edge from router_node.
    Returns a list of Send() objects telling LangGraph which agents to run.
    
    Single branch → 1 Send  (only relevant agent runs)
    Cross branch  → 3 Sends (all three agents run IN PARALLEL)
    """
    branch = state["branch"]

    if branch == "housing":
        return [Send("housing_agent", state)]

    elif branch == "transportation":
        return [Send("transport_agent", state)]

    elif branch == "crime":
        return [Send("crime_agent", state)]

    else:  # cross or general
        # All three agents run simultaneously
        return [
            Send("housing_agent",   state),
            Send("transport_agent", state),
            Send("crime_agent",     state),
        ]


# ---------------------------------------------------------------------------
# RAG Helper
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
# Housing Analyst Functions
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

def housing_price_analyst(query_text=""):
    query_lower = query_text.lower()
    
    # 如果问题涉及商业/投资，包含所有地产类型
    if any(w in query_lower for w in ['commercial', 'investment', 'office', 
                                       'retail', 'business', 'all property', 'all types']):
        category_filter = ""
        note = "all property types"
    else:
        category_filter = "AND pt.CATEGORY = 'residential'"
        note = "residential only"
    
    results = session.sql(f"""
        SELECT 
            n.NEIGHBORHOOD_NAME,
            COUNT(f.PROPERTY_ID) AS TOTAL_PROPERTIES,
            ROUND(AVG(f.TOTAL_VALUE), 2) AS AVG_PROPERTY_VALUE,
            ROUND(MEDIAN(f.TOTAL_VALUE), 2) AS MEDIAN_PROPERTY_VALUE,
            ROUND(AVG(f.PRICE_PER_SQFT), 2) AS AVG_PRICE_PER_SQFT
        FROM {Tables.HOUSING_FACT_PROPERTY} f
        JOIN {Tables.HOUSING_DIM_PROPERTY_TYPE} pt 
            ON f.PROPERTY_TYPE_KEY = pt.PROPERTY_TYPE_KEY
        JOIN {DB}.HOUSING_CORE.DIM_NEIGHBORHOOD n
            ON f.NEIGHBORHOOD_KEY = n.NEIGHBORHOOD_KEY
        WHERE f.TOTAL_VALUE > 0
          AND f.PRICE_PER_SQFT > 0
          {category_filter}
        GROUP BY n.NEIGHBORHOOD_NAME
        ORDER BY AVG_PROPERTY_VALUE DESC
        LIMIT 10
    """).to_pandas()
    
    return [{'neighborhood': r['NEIGHBORHOOD_NAME'],
             'avg_value': float(r['AVG_PROPERTY_VALUE']),
             'median_value': float(r['MEDIAN_PROPERTY_VALUE']),
             'avg_price_per_sqft': float(r['AVG_PRICE_PER_SQFT']),
             'total_properties': int(r['TOTAL_PROPERTIES']),
             'data_scope': note}
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
# Transportation Analyst Functions
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
# Crime Analyst Functions
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
# Cross-branch Analyst (neighborhood + district + station mapping)
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
# Node 2a: Housing Agent
# ---------------------------------------------------------------------------

def housing_agent_node(state: CityLensState) -> dict:
    """
    Runs all housing-related analysts.
    Returns results in agent_results list.
    operator.add merges this with other agents' results.
    """
    query_text = state["user_query"]
    intent     = state["intent"]

    data = {}
    data["neighborhood"] = housing_neighborhood_analyst(query_text)
    data["qa_context"]   = housing_qa_analyst(query_text)
    data["price"]        = housing_price_analyst()
    data["price"] = housing_price_analyst(query_text)

    query_lower = state["user_query"].lower()
    if intent == "building_type" or any(w in query_lower for w in ['condo', 'single family', 'two family']):
        data["building_type"] = housing_building_type_analyst()

    total = sum(len(v) for v in data.values())
    print(f"  🏠 Housing agent done: {total} items")

    return {"agent_results": [{"branch": "housing", "data": data}]}


# ---------------------------------------------------------------------------
# Node 2b: Transport Agent
# ---------------------------------------------------------------------------

def transport_agent_node(state: CityLensState) -> dict:
    """
    Runs all transportation-related analysts.
    Returns results in agent_results list.
    """
    query_text = state["user_query"]
    intent     = state["intent"]
    found_line = state["entities"].get("line", "")

    data = {}
    data["performance"] = transport_performance_analyst(query_text, found_line)
    data["alerts"]      = transport_alerts_analyst(found_line)

    if intent in ["reliability", "general"]:
        data["reliability"] = transport_reliability_analyst(query_text, found_line)
    if intent in ["anomaly", "alerts", "general"]:
        data["anomaly"] = transport_anomaly_analyst(query_text, found_line)
    if intent == "weather":
        data["weather"] = transport_weather_analyst(query_text, found_line)
    if intent in ["station", "best_time"]:
        data["station"] = transport_station_analyst(query_text, found_line)
    if intent == "trend":
        data["monthly"] = transport_monthly_analyst(query_text, found_line)

    total = sum(len(v) for v in data.values())
    print(f"  🚇 Transport agent done: {total} items")

    return {"agent_results": [{"branch": "transportation", "data": data}]}


# ---------------------------------------------------------------------------
# Node 2c: Crime Agent
# ---------------------------------------------------------------------------

def crime_agent_node(state: CityLensState) -> dict:
    """
    Runs all crime-related analysts.
    Returns results in agent_results list.
    """
    query_text = state["user_query"]
    intent     = state["intent"]

    data = {}
    data["offense"]  = crime_offense_analyst(query_text)
    data["district"] = crime_district_analyst()

    if intent == "trend":
        data["trend"] = crime_trend_analyst()
    if intent == "shooting":
        data["shooting"] = crime_shooting_analyst()

    total = sum(len(v) for v in data.values())
    print(f"  🚨 Crime agent done: {total} items")

    return {"agent_results": [{"branch": "crime", "data": data}]}


# ---------------------------------------------------------------------------
# Node 3: Aggregator
# ---------------------------------------------------------------------------

def aggregator_node(state: CityLensState) -> dict:
    """
    Collects results from all parallel agents (via agent_results).
    Consolidates into raw_context for Synthesis node.
    For cross-branch queries, also runs cross_branch_analyst()
    to add the neighborhood-district-station mapping data.
    """
    raw_context = {}
    total = 0

    # agent_results is a list like:
    # [{"branch": "housing", "data": {...}},
    #  {"branch": "transportation", "data": {...}},
    #  {"branch": "crime", "data": {...}}]
    for agent_result in state["agent_results"]:
        branch = agent_result["branch"]
        data   = agent_result["data"]
        for key, items in data.items():
            # prefix with branch name to avoid key collisions
            raw_context[f"{branch}_{key}"] = items
            total += len(items)
            print(f"  📊 {branch}/{key}: {len(items)} items")

    # For cross-branch: add neighborhood mapping data
    if state["branch"] == "cross":
        cross_data = cross_branch_analyst()
        raw_context["cross_mapping"] = cross_data
        total += len(cross_data)
        print(f"  🔀 Cross mapping: {len(cross_data)} neighborhoods")

    return {"raw_context": raw_context, "total_retrievals": total}


# ---------------------------------------------------------------------------
# Node 4: Synthesis
# ---------------------------------------------------------------------------

BRANCH_PROMPTS = {
    "housing":        "You are a Boston Housing Intelligence Analyst.Note: property values in this dataset reflect residential properties only from Boston's official assessment records.",
    "transportation": "You are a senior Boston MBTA Transportation Analyst.",
    "crime":          "You are a Boston Crime Intelligence Analyst specializing in public safety data.",
    "cross":          "You are a Boston Urban Intelligence Analyst with expertise in housing, transportation, and crime data.",
}
def compress_item(item: dict) -> dict:
    exclude_keys = {'summary', 'similarity', 'housing_summary'}
    return {k: v for k, v in item.items() if k not in exclude_keys}

def synthesis_node(state: CityLensState) -> dict:
    context_text = ""
    for analyst_name, items in state["raw_context"].items():
        context_text += f"\n=== {analyst_name.upper()} ===\n"
        for item in items:
            context_text += json.dumps(compress_item(item), default=safe_serialize) + "\n"
    context_text = context_text[:12000]
    
    history_context = ""
    if state.get("conversation_history") and len(state["conversation_history"]) > 0:
        history_context = "\n\nPrevious conversation:\n"
        for h in state["conversation_history"][-4:]:
            role_label = "User" if h["role"] == "user" else "Assistant"
            history_context += f"{role_label}: {h['content'][:150]}\n"
        history_context += "\n"

    role = BRANCH_PROMPTS.get(state["branch"], "You are a Boston city intelligence analyst.")
    
    multistep_context = ""
    if state.get("use_multistep") and state.get("sub_questions"):
        multistep_context = "\n\nThis complex question was broken into sub-questions:\n"
        for i, sq in enumerate(state["sub_questions"]):
            multistep_context += f"{i+1}. {sq}\n"
        multistep_context += "Please synthesize all data to answer comprehensively.\n"

    prompt = f"""{role}

Question: {state['user_query']}
{multistep_context}

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

    return {"answer": answer, "latency_ms": latency_ms}


# ---------------------------------------------------------------------------
# Node 5: Reflection
# ---------------------------------------------------------------------------

def reflection_node(state: CityLensState) -> dict:
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

    # Confidence Score
    raw_context = state.get("raw_context", {})
    total_items = sum(len(v) for v in raw_context.values())
    
    # 基于 retrieval 量和 branch 确定性
    if total_items >= 50:
        data_confidence = 1.0
    elif total_items >= 20:
        data_confidence = 0.8
    elif total_items >= 10:
        data_confidence = 0.6
    else:
        data_confidence = 0.4

    branch = state.get("branch", "")
    scores_sum = state.get("total_retrievals", 0)
    branch_confidence = 0.9 if branch != "cross" else 0.75

    confidence = round((data_confidence + branch_confidence) / 2, 2)

    print(f"  ⭐ Reflection score: {score}/100")
    print(f"  🎯 Confidence: {confidence}")

    return {"reflection_score": score, "final_answer": answer, 
            "confidence_score": confidence}


# ---------------------------------------------------------------------------
# Build the LangGraph
# ---------------------------------------------------------------------------

def build_graph():
    graph = StateGraph(CityLensState)

    # Register all nodes
    graph.add_node("router",          router_node)
    graph.add_node("housing_agent",   housing_agent_node)
    graph.add_node("transport_agent", transport_agent_node)
    graph.add_node("crime_agent",     crime_agent_node)
    graph.add_node("aggregator",      aggregator_node)
    graph.add_node("synthesis",       synthesis_node)
    graph.add_node("reflection",      reflection_node)
    graph.add_node("decompose", decompose_node)

    # Entry point
    graph.set_entry_point("router")
    graph.add_edge("router", "decompose")

    # Router → agents (conditional parallel dispatch via Send)
    graph.add_conditional_edges(
    "decompose",                              # 改这行（原来是 "router"）
    route_to_agents,
    ["housing_agent", "transport_agent", "crime_agent"]
)

    # All agents → aggregator
    graph.add_edge("housing_agent",   "aggregator")
    graph.add_edge("transport_agent", "aggregator")
    graph.add_edge("crime_agent",     "aggregator")

    # Linear flow after aggregation
    graph.add_edge("aggregator",  "synthesis")
    graph.add_edge("synthesis",   "reflection")
    graph.add_edge("reflection",  END)

    return graph.compile()


citylens_graph = build_graph()
print("✅ LangGraph parallel multi-agent compiled successfully!")


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------



def run_citylens(user_query: str) -> str:
    global _conversation_history

    print(f"\n{'='*60}")
    print(f"❓ {user_query}")
    print('='*60)

    # Check cache
    cache_key = user_query.lower().strip()
    if cache_key in _query_cache:
        cached = _query_cache[cache_key]
        print("  ⚡ Cache hit! Returning cached answer.")
        cached = _query_cache[cache_key]
        print(f"\n{'='*60}")
        print("🤖 FINAL ANSWER (cached):")
        print(cached["answer"])
        print(f"\n⏱  Latency    : 0ms (cached)")
        print(f"🎯 Confidence : {cached['confidence']}")
        return {
            "answer":        cached["answer"],
            "branch":        cached.get("branch", ""),
            "intent":        cached.get("intent", ""),
            "confidence":    cached["confidence"],
            "latency_ms":    0,
            "retrievals":    cached.get("retrievals", 0),
            "sub_questions": cached.get("sub_questions", []),
            "use_multistep": cached.get("use_multistep", False),
            "cached":        True
    }

    initial_state: CityLensState = {
        "user_query":       user_query,
        "query_id":         str(uuid.uuid4()),
        "query_ts":         datetime.now().isoformat(),
        "branch":           "",
        "intent":           "",
        "entities":         {},
        "agent_results":    [],
        "raw_context":      {},
        "total_retrievals": 0,
        "answer":           "",
        "latency_ms":       0,
        "reflection_score": 0,
        "confidence_score": 0.0,
        "final_answer":     "",
        "sub_questions":    [],
        "use_multistep":    False,
    }

    result = citylens_graph.invoke(initial_state)

    # Save to cache
    if len(_query_cache) >= CACHE_MAX_SIZE:
        oldest_key = next(iter(_query_cache))
        del _query_cache[oldest_key]
    
    _query_cache[cache_key] = {
        "answer":     result["final_answer"],
        "confidence": result["confidence_score"],
        "branch":        result["branch"],
    "intent":        result["intent"],
    "retrievals":    result["total_retrievals"],
    "sub_questions": result.get("sub_questions", []),
    "use_multistep": result.get("use_multistep", False),
    }

    print(f"\n{'='*60}")
    print("🤖 FINAL ANSWER:")
    print(result["final_answer"])
    print(f"\n⏱  Latency    : {result['latency_ms']}ms")
    print(f"📊 Retrievals : {result['total_retrievals']}")
    print(f"⭐ Score      : {result['reflection_score']}/100")
    print(f"🎯 Confidence : {result['confidence_score']}")
    print(f"🌿 Branch     : {result['branch']}")

    return {
        "answer":        result["final_answer"],
        "branch":        result["branch"],
        "intent":        result["intent"],
        "confidence":    result["confidence_score"],
        "latency_ms":    result["latency_ms"],
        "retrievals":    result["total_retrievals"],
        "sub_questions": result.get("sub_questions", []),
        "use_multistep": result.get("use_multistep", False),
        "cached":        False
}



# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_citylens("What are the most expensive neighborhoods in Boston?")
    run_citylens("What about crime rates in those neighborhoods?")
    run_citylens("And how is the transit access there?")
 
    
