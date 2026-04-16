import re
import uuid
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake_config import SNOWFLAKE_CONN

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CityLens Boston",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Snowflake session ─────────────────────────────────────────────────────────
@st.cache_resource
def get_session():
    return Session.builder.configs(SNOWFLAKE_CONN).create()

session = get_session()
DB = "CITYLENS_MERGED_DB"

DISTRICT_MAP = {
    "A1": "Downtown", "A7": "East Boston", "A15": "Charlestown",
    "B2": "Roxbury", "B3": "Mattapan", "C6": "South Boston",
    "C11": "Dorchester", "D4": "South End", "D14": "Brighton/Allston",
    "E5": "West Roxbury", "E13": "Jamaica Plain", "E18": "Hyde Park"
}

def replace_districts(text):
    for code, name in DISTRICT_MAP.items():
        text = re.sub(rf"\b{code}\b", f"{name} ({code})", text)
    return text

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Inter:wght@300;400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
header[data-testid="stHeader"] {
    background-color: transparent !important;
    box-shadow: none !important;
}

.stApp { background-color: #f7f6f2; }

[data-testid="stSidebar"] {
    background-color: #ffffff;
    border-right: 1px solid #e8e6e0;
}
[data-testid="stSidebar"] * { color: #3a3a3a !important; }

.stButton > button {
    background: #f7f6f2 !important;
    color: #3a3a3a !important;
    border: 1px solid #e0ddd6 !important;
    border-radius: 8px !important;
    font-size: 0.82rem !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 400 !important;
    text-align: left !important;
    padding: 0.5rem 0.8rem !important;
    transition: all 0.15s ease !important;
    box-shadow: none !important;
}
.stButton > button:hover {
    background: #eeecea !important;
    border-color: #c8c4bc !important;
}

div[data-testid="column"]:last-child .stButton > button {
    background: #1a1a1a !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 0.9rem !important;
    padding: 0.65rem 1.4rem !important;
}
div[data-testid="column"]:last-child .stButton > button:hover {
    background: #333333 !important;
}

.stTextInput > div > div > input {
    background: #ffffff !important;
    border: 1px solid #dddad4 !important;
    border-radius: 10px !important;
    color: #1a1a1a !important;
    font-size: 0.97rem !important;
    font-family: 'Inter', sans-serif !important;
    padding: 0.75rem 1rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
    outline: none !important;
}
.stTextInput > div > div > input:focus {
    border-color: #1a1a1a !important;
    box-shadow: 0 0 0 2px rgba(26,26,26,0.08) !important;
    outline: none !important;
}
.stTextInput label { display: none !important; }

.hero-wrap { padding: 2.5rem 0 1.5rem; }
.hero-title {
    font-family: 'Instrument Serif', serif;
    font-size: 3.2rem;
    color: #1a1a1a;
    line-height: 1.1;
    margin-bottom: 0.4rem;
    font-weight: 400;
}
.hero-title em { font-style: italic; color: #6b7280; }
.hero-sub {
    font-size: 0.8rem;
    color: #9ca3af;
    letter-spacing: 0.12em;
    text-transform: uppercase;
}

.branch-tag {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    border-radius: 99px;
    font-size: 0.73rem;
    font-weight: 500;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    margin-right: 6px;
    margin-bottom: 12px;
}
.tag-housing        { background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }
.tag-transportation { background: #eff6ff; color: #1e40af; border: 1px solid #bfdbfe; }
.tag-crime          { background: #fff1f2; color: #9f1239; border: 1px solid #fecdd3; }
.tag-cross          { background: #faf5ff; color: #6b21a8; border: 1px solid #e9d5ff; }

.answer-card {
    background: #ffffff;
    border: 1px solid #e8e6e0;
    border-radius: 14px;
    padding: 1.8rem 2rem;
    color: #2d2d2d;
    font-size: 0.95rem;
    line-height: 1.8;
    box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    margin-top: 0.5rem;
}

.metrics-row { display: flex; gap: 10px; margin-top: 1rem; }
.metric-card {
    flex: 1;
    background: #ffffff;
    border: 1px solid #e8e6e0;
    border-radius: 10px;
    padding: 0.9rem 1rem;
    text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,0.03);
}
.metric-num { font-size: 1.5rem; font-weight: 600; color: #1a1a1a; line-height: 1.2; }
.metric-unit { font-size: 0.75rem; color: #9ca3af; font-weight: 400; }
.metric-label { font-size: 0.7rem; color: #9ca3af; text-transform: uppercase; letter-spacing: 0.08em; margin-top: 3px; }
.score-bar { height: 3px; background: #f0ede8; border-radius: 99px; margin-top: 8px; overflow: hidden; }
.score-fill { height: 100%; border-radius: 99px; background: #1a1a1a; }

.hist-item { padding: 0.6rem 0; border-bottom: 1px solid #f0ede8; font-size: 0.8rem; }
.hist-q { color: #3a3a3a; font-weight: 500; margin-bottom: 2px; }
.hist-meta { color: #b0a89c; font-size: 0.72rem; }

.pipe-item { display: flex; align-items: center; gap: 8px; font-size: 0.78rem; color: #9ca3af; padding: 4px 0; }
.pipe-dot { width: 6px; height: 6px; border-radius: 50%; background: #d1d5db; flex-shrink: 0; }
.div-line { border-top: 1px solid #f0ede8; margin: 1.2rem 0; }
.sidebar-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: #b0a89c; font-weight: 600; margin-bottom: 8px; margin-top: 4px; }

.empty-state { text-align: center; padding: 4rem 0; }
.empty-icon { font-size: 2.5rem; margin-bottom: 1rem; }
.empty-text { font-family: 'Instrument Serif', serif; font-size: 1.4rem; color: #c8c4bc; font-weight: 400; }
.empty-sub { font-size: 0.8rem; color: #d1cdc7; margin-top: 0.4rem; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "history" not in st.session_state:
    st.session_state.history = []
if "prefill" not in st.session_state:
    st.session_state.prefill = ""

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding: 0.8rem 0 1.4rem'>
        <div style='font-family: Instrument Serif, serif; font-size: 1.35rem; color: #1a1a1a; margin-bottom: 2px;'>CityLens</div>
        <div style='font-size: 0.75rem; color: #b0a89c; letter-spacing: 0.05em;'>Boston Urban Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div class='sidebar-label'>Try asking</div>", unsafe_allow_html=True)

    samples = [
        ("🏠", "What are the most expensive neighborhoods?"),
        ("🚇", "Which MBTA line is most reliable?"),
        ("🚨", "Which districts have highest crime rates?"),
        ("🗺️", "Where should I live in Boston?"),
        ("💰", "Are there affordable and safe neighborhoods?"),
        ("⏰", "Best time to take the Green Line?"),
    ]

    for icon, q in samples:
        if st.button(f"{icon}  {q}", key=f"s_{q}", use_container_width=True):
            st.session_state.prefill = q
            st.rerun()

    st.markdown("<div class='div-line'></div>", unsafe_allow_html=True)
    st.markdown("<div class='sidebar-label'>Pipeline</div>", unsafe_allow_html=True)
    st.markdown("""
    <div style='margin-top:4px'>
        <div class='pipe-item'><div class='pipe-dot'></div>Router — branch detection</div>
        <div class='pipe-item'><div class='pipe-dot'></div>Parallel agents — data fetch</div>
        <div class='pipe-item'><div class='pipe-dot'></div>Aggregator — merge results</div>
        <div class='pipe-item'><div class='pipe-dot'></div>Synthesis — Cortex AI</div>
        <div class='pipe-item'><div class='pipe-dot'></div>Reflection — quality score</div>
    </div>
    """, unsafe_allow_html=True)

    if st.session_state.history:
        st.markdown("<div class='div-line'></div>", unsafe_allow_html=True)
        st.markdown("<div class='sidebar-label'>Recent</div>", unsafe_allow_html=True)
        for item in reversed(st.session_state.history[-4:]):
            dot_color = {
                "housing": "#065f46", "transportation": "#1e40af",
                "crime": "#9f1239", "cross": "#6b21a8"
            }.get(item.get("branch", ""), "#9ca3af")
            st.markdown(f"""
            <div class='hist-item'>
                <div class='hist-q'>{item['query'][:52]}{'…' if len(item['query']) > 52 else ''}</div>
                <div class='hist-meta'>
                    <span style='color:{dot_color}'>●</span>
                    {item.get('branch', '—')} · {item.get('latency', 0)}ms · {item.get('ts', '')}
                </div>
            </div>
            """, unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class='hero-wrap'>
    <div class='hero-title'>City<em>Lens</em></div>
    <div class='hero-sub'>Boston · Housing · Crime · Transit</div>
</div>
""", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_qa, tab_crime, tab_mbta = st.tabs(["🤖 AI Assistant", "🚨 Crime Dashboard", "🚇 MBTA Dashboard"])

# =============================================================================
# TAB 1: AI Assistant
# =============================================================================
with tab_qa:
    col_q, col_btn = st.columns([6, 1])
    with col_q:
        query = st.text_input(
            "q",
            label_visibility="collapsed",
            placeholder="Ask about Boston housing, crime, or transit…",
            key="query_box",
        )
    with col_btn:
        ask = st.button("Ask →", use_container_width=True)

    # 点击示例问题直接触发查询
    if st.session_state.prefill:
        query = st.session_state.prefill
        st.session_state.prefill = ""
        ask = True

    if ask and query.strip():
        with st.spinner("Analyzing Boston data…"):
            try:
                from citylens_langgraph import citylens_graph

                initial_state = {
                    "user_query":       query,
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
                    "final_answer":     "",
                }

                result = citylens_graph.invoke(initial_state)

                branch    = result.get("branch", "cross")
                intent    = result.get("intent", "general")
                latency   = result.get("latency_ms", 0)
                score     = result.get("reflection_score", 0)
                total_ret = result.get("total_retrievals", 0)
                answer    = result.get("final_answer", "No answer returned.")
                answer    = replace_districts(answer)

                st.session_state.history.append({
                    "query": query, "branch": branch,
                    "latency": latency, "score": score,
                    "ts": datetime.now().strftime("%H:%M"),
                })

                tag_map = {
                    "housing":        ("🏠 Housing",        "tag-housing"),
                    "transportation": ("🚇 Transportation", "tag-transportation"),
                    "crime":          ("🚨 Crime",          "tag-crime"),
                    "cross":          ("🗺️ Cross-domain",   "tag-cross"),
                }
                tag_label, tag_cls = tag_map.get(branch, ("◆ " + branch, "tag-cross"))

                st.markdown(f"""
                <div style='margin-top:1.5rem; margin-bottom:0.2rem;'>
                    <span class='branch-tag {tag_cls}'>{tag_label}</span>
                    <span class='branch-tag' style='background:#f7f6f2; color:#9ca3af; border:1px solid #e8e6e0;'>
                        {intent.replace("_", " ").title()}
                    </span>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("<div class='answer-card'>", unsafe_allow_html=True)
                st.markdown(answer)
                st.markdown("</div>", unsafe_allow_html=True)

                st.markdown(f"""
                <div class='metrics-row'>
                    <div class='metric-card'>
                        <div class='metric-num'>{latency}<span class='metric-unit'> ms</span></div>
                        <div class='metric-label'>Latency</div>
                    </div>
                    <div class='metric-card'>
                        <div class='metric-num'>{total_ret}</div>
                        <div class='metric-label'>Data Points</div>
                    </div>
                    <div class='metric-card'>
                        <div class='metric-num'>{score}<span class='metric-unit'>/100</span></div>
                        <div class='metric-label'>Quality Score</div>
                        <div class='score-bar'>
                            <div class='score-fill' style='width:{score}%'></div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            except ImportError:
                st.error("⚠️ Cannot import `citylens_langgraph`. Make sure it's in the same folder as app.py.")
            except Exception as e:
                st.error(f"⚠️ Error: {e}")

    elif ask:
        st.warning("Please enter a question.")

    if not st.session_state.history and not (ask and query.strip()):
        st.markdown("""
        <div class='empty-state'>
            <div class='empty-icon'>🏙️</div>
            <div class='empty-text'>Ask anything about Boston</div>
            <div class='empty-sub'>Housing · Crime · Transit · Neighborhoods</div>
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# TAB 2: Crime Dashboard
# =============================================================================
with tab_crime:
    st.markdown("### 🚨 Boston Crime Dashboard")

    @st.cache_data(ttl=300)
    def load_crime_data():
        return session.sql(f"""
            SELECT DISTRICT, LAT, LONG, OCCURRED_ON_DATE
            FROM {DB}.CRIME_PUBLIC.CRIME_RAW
            WHERE LAT IS NOT NULL AND LONG IS NOT NULL
            AND OCCURRED_ON_DATE >= DATEADD(year, -5, CURRENT_DATE)
        """).to_pandas()

    df = load_crime_data()

    if df.empty:
        st.warning("No crime data found.")
    else:
        df["OCCURRED_ON_DATE"] = pd.to_datetime(df["OCCURRED_ON_DATE"])
        df["year"] = df["OCCURRED_ON_DATE"].dt.year

        # ── Filters ──────────────────────────────────────────────────────────
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            district_list = sorted(df["DISTRICT"].dropna().unique())
            selected_display = st.selectbox(
                "📍 Select Area",
                ["All"] + [f"{DISTRICT_MAP.get(d, d)} ({d})" for d in district_list]
            )
            selected_district = (
                "All" if selected_display == "All"
                else selected_display.split("(")[-1].replace(")", "").strip()
            )
        with col_f2:
            years = sorted(df["year"].dropna().unique(), reverse=True)
            selected_year = st.selectbox("📅 Select Year", ["All"] + [int(y) for y in years])

        df_filtered = df.copy()
        if selected_district != "All":
            df_filtered = df_filtered[df_filtered["DISTRICT"] == selected_district]
        if selected_year != "All":
            df_filtered = df_filtered[df_filtered["year"] == selected_year]

        # ── KPIs ─────────────────────────────────────────────────────────────
        agg = df_filtered["DISTRICT"].value_counts().reset_index()
        agg.columns = ["DISTRICT", "crime_count"]
        agg["district_name"] = agg["DISTRICT"].map(DISTRICT_MAP).fillna(agg["DISTRICT"])

        most   = agg.iloc[0] if len(agg) > 0 else None
        # 过滤掉不在 DISTRICT_MAP 里的行
        agg_filtered = agg[agg["DISTRICT"].isin(DISTRICT_MAP.keys())]
        safest = agg_filtered.iloc[-1] if len(agg_filtered) > 1 else most

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Incidents", f"{len(df_filtered):,}")
        c2.metric("🔴 Highest Risk", most["district_name"] if most is not None else "—")
        c3.metric("🟢 Lowest Risk",  safest["district_name"] if safest is not None else "—")

        st.divider()

        # ── Map + Ranking ─────────────────────────────────────────────────────
        left, right = st.columns([2, 1])

        with left:
            st.markdown("#### 🗺️ Crime Map")

            map_df = df_filtered.rename(columns={"LAT": "lat", "LONG": "lon"})
            map_df = map_df.dropna(subset=["lat", "lon", "DISTRICT"])
            if len(map_df) > 800:
                map_df = map_df.sample(800, random_state=42)

            # Risk level calculation
            counts = df_filtered["DISTRICT"].value_counts()
            q66 = counts.quantile(0.66)
            q33 = counts.quantile(0.33)

            def risk_level(d):
                c = counts.get(d, 0)
                return "High" if c > q66 else ("Medium" if c > q33 else "Low")

            map_df = map_df.copy()
            map_df["risk"] = map_df["DISTRICT"].apply(risk_level)

            risk_filter = st.radio(
                "Filter by Risk Level",
                ["All", "High", "Medium", "Low"],
                horizontal=True
            )

            if risk_filter == "High":
                st.error("🔴 Showing HIGH risk areas only")
            elif risk_filter == "Medium":
                st.warning("🟠 Showing MEDIUM risk areas only")
            elif risk_filter == "Low":
                st.success("🟢 Showing LOW risk areas only")

            if risk_filter != "All":
                map_df = map_df[map_df["risk"] == risk_filter]

            import pydeck as pdk
            import json

            @st.cache_data(ttl=300)
            def load_neighborhood_geojson():
                rows = session.sql(f"""
                    SELECT 
                        NEIGHBORHOOD_NAME,
                        ST_ASGEOJSON(
                            ST_TRANSFORM(
                                TO_GEOMETRY(
                                    '{{"type":"' || GEOMETRY_TYPE || '","coordinates":' || GEOMETRY_COORDINATES::STRING || '}}'
                                ),
                                2249,
                                4326
                            )
                        )::STRING AS GEOJSON
                    FROM CITYLENS_MERGED_DB.HOUSING_CORE.DIM_NEIGHBORHOOD
                """).collect()
                return rows

            try:
                geo_rows = load_neighborhood_geojson()
                crime_counts = df_filtered["DISTRICT"].value_counts().to_dict()
                max_count = max(crime_counts.values()) if crime_counts else 1

                nbhd_to_district = {}
                for code, name in DISTRICT_MAP.items():
                    nbhd_to_district[name.upper()] = code

                # 手动补充没有匹配到的 neighborhood
                nbhd_to_district.update({
                    "ALLSTON":          "D14",
                    "BRIGHTON":         "D14",
                    "DOWNTOWN":         "A1",
                    "BEACON HILL":      "A1",
                    "WEST END":         "A1",
                    "NORTH END":        "A1",
                    "CHINATOWN":        "A1",
                    "LEATHER DISTRICT": "A1",
                    "BAY VILLAGE":      "D4",
                    "BACK BAY":         "D4",
                    "FENWAY":           "D4",
                    "LONGWOOD":         "D4",
                    "MISSION HILL":     "E13",
                    "CHARLESTOWN":      "A15",
                    "EAST BOSTON":      "A7",
                    "SOUTH BOSTON":     "C6",
                    "SOUTH BOSTON WATERFRONT": "C6",
                    "SOUTH END":        "D4",
                    "ROXBURY":          "B2",
                    "DORCHESTER":       "C11",
                    "MATTAPAN":         "B3",
                    "JAMAICA PLAIN":    "E13",
                    "ROSLINDALE":       "E5",
                    "WEST ROXBURY":     "E5",
                    "HYDE PARK":        "E18",
                })

                features = []
                for row in geo_rows:
                    nbhd = row["NEIGHBORHOOD_NAME"]
                    try:
                        geom = json.loads(row["GEOJSON"])
                    except:
                        continue

                    district_code = nbhd_to_district.get(nbhd.upper())
                    count = crime_counts.get(district_code, 0)
                    ratio = count / max_count if max_count > 0 else 0

                    r_val = int(50  + ratio * 200)
                    g_val = int(180 - ratio * 150)
                    alpha = 180

                    if risk_filter != "All" and district_code:
                        alpha = 180 if risk_level(district_code) == risk_filter else 30

                    features.append({
                        "type": "Feature",
                        "geometry": geom,
                        "properties": {
                            "neighborhood": nbhd,
                            "district": district_code or "—",
                            "crime_count": int(count),
                            "fill_color": [r_val, g_val, 80, alpha],
                        }
                    })

                geo_layer = pdk.Layer(
                    "GeoJsonLayer",
                    data={"type": "FeatureCollection", "features": features},
                    get_fill_color="properties.fill_color",
                    get_line_color=[255, 255, 255, 200],
                    line_width_min_pixels=1,
                    pickable=True,
                    auto_highlight=True,
                )

                view = pdk.ViewState(latitude=42.33, longitude=-71.07, zoom=11, pitch=0)

                tooltip = {
                    "html": "<b>{neighborhood}</b><br/>📍 District: {district}<br/>🚨 Incidents: {crime_count}",
                    "style": {
                        "backgroundColor": "#1a1a1a",
                        "color": "white",
                        "fontSize": "13px",
                        "padding": "10px",
                        "borderRadius": "8px",
                    }
                }

                st.pydeck_chart(pdk.Deck(
                    layers=[geo_layer],
                    initial_view_state=view,
                    tooltip=tooltip,
                    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
                ))

                st.markdown("🟢 Safer &nbsp;&nbsp; 🟡 Medium &nbsp;&nbsp; 🔴 Higher concern")
                st.caption("Hover over a neighborhood to see details")

            except Exception as e:
                st.warning(f"Map unavailable: {e}")

            top_districts = map_df["DISTRICT"].value_counts().head(3)
            if not top_districts.empty:
                top_names = [f"{DISTRICT_MAP.get(d, d)} ({d})" for d in top_districts.index]
                st.info(f"📍 Most active areas: {', '.join(top_names)}")

        with right:
            st.markdown("#### 🚨 Risk Ranking")
            for i, (_, row) in enumerate(agg.head(8).iterrows()):
                medal = ["🥇","🥈","🥉"][i] if i < 3 else f"{i+1}."
                st.markdown(f"{medal} **{row['district_name']}** — {row['crime_count']:,}")

        st.divider()

        # ── Trend Chart ───────────────────────────────────────────────────────
        st.markdown("#### 📊 Crime Trend")
        if selected_district == "All":
            st.bar_chart(agg.set_index("district_name")["crime_count"])
        else:
            df_recent = df_filtered[
                df_filtered["OCCURRED_ON_DATE"] >= datetime.now() - timedelta(days=90)
            ].copy()
            df_recent["week"] = df_recent["OCCURRED_ON_DATE"].dt.to_period("W").astype(str)
            trend = df_recent.groupby("week").size()
            if not trend.empty:
                st.bar_chart(trend)
            else:
                st.info("No recent data available for this selection.")

# =============================================================================
# TAB 3: MBTA Dashboard
# =============================================================================
with tab_mbta:
    st.markdown("### 🚇 MBTA Transportation Dashboard")

    # ── Data loaders ─────────────────────────────────────────────────────────
    @st.cache_data(ttl=300)
    def load_weekly_events():
        try:
            return session.sql(f"""
                SELECT ROUTE_ID, WEEK_START_DATE, TOTAL_EVENTS, UNIQUE_TRIPS
                FROM {DB}.CITYLENS_MART.MART_ROUTE_WEEKLY
                ORDER BY WEEK_START_DATE
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_reliability():
        try:
            return session.sql(f"""
                SELECT ROUTE_ID, RELIABILITY_PCT, RELIABILITY_GRADE, BAD_DAYS, TOTAL_DAYS
                FROM {DB}.CITYLENS_SERVING.SRV_ROUTE_RELIABILITY
                ORDER BY RELIABILITY_PCT DESC
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_dow():
        try:
            return session.sql(f"""
                SELECT DAY_OF_WEEK, ROUND(AVG(AVG_EVENTS), 0) AS AVG_EVENTS
                FROM {DB}.CITYLENS_SERVING.SRV_DAYOFWEEK_PERFORMANCE
                GROUP BY 1 ORDER BY AVG_EVENTS DESC
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_weather():
        try:
            return session.sql(f"""
                SELECT WEATHER_CONDITION,
                       COUNT(*) AS DAYS,
                       ROUND(AVG(TOTAL_EVENTS), 0) AS AVG_EVENTS,
                       ROUND(AVG(UNIQUE_TRIPS), 0) AS AVG_TRIPS
                FROM {DB}.CITYLENS_MART.MART_ROUTE_WEATHER
                GROUP BY 1 ORDER BY AVG_EVENTS DESC
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_temp():
        try:
            return session.sql(f"""
                SELECT TEMP_CATEGORY, ROUND(AVG(TOTAL_EVENTS), 0) AS AVG_EVENTS, COUNT(*) AS DAYS
                FROM {DB}.CITYLENS_MART.MART_ROUTE_WEATHER
                GROUP BY 1 ORDER BY AVG_EVENTS DESC
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_alerts():
        try:
            return session.sql(f"""
                SELECT ROUTE_ID, CAUSE, EFFECT,
                       SUM(ALERT_COUNT) AS TOTAL_ALERTS,
                       SUM(ACTIVE_ALERTS) AS ACTIVE_ALERTS
                FROM {DB}.CITYLENS_MART.MART_ALERTS_SUMMARY
                WHERE ROUTE_ID IS NOT NULL
                GROUP BY 1,2,3
                ORDER BY TOTAL_ALERTS DESC
                LIMIT 20
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_anomalies():
        try:
            return session.sql(f"""
                SELECT ROUTE_ID, SERVICE_DATE, ANOMALY_TYPE,
                       LIKELY_CAUSE, PCT_FROM_BASELINE, WEATHER_CONDITION
                FROM {DB}.CITYLENS_MART.MART_ANOMALY_DETECTION
                WHERE ANOMALY_TYPE != 'NORMAL'
                ORDER BY ABS(Z_SCORE) DESC
                LIMIT 15
            """).to_pandas()
        except:
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_stations():
        try:
            return session.sql(f"""
                SELECT s.STATION_NAME, s.LINE_NAME, s.MUNICIPALITY,
                       s.LATITUDE, s.LONGITUDE,
                       COUNT(e.EVENT_TYPE)            AS TOTAL_EVENTS,
                       COUNT(DISTINCT e.TRIP_ID)      AS UNIQUE_TRIPS,
                       COUNT(DISTINCT e.SERVICE_DATE) AS DAYS_WITH_SERVICE
                FROM {DB}.CITYLENS_CORE.DIM_STATION s
                LEFT JOIN {DB}.CITYLENS_CORE.FACT_EVENTS e ON s.STATION_ID = e.STOP_ID
                GROUP BY 1,2,3,4,5
                HAVING s.LATITUDE IS NOT NULL
                ORDER BY TOTAL_EVENTS DESC
            """).to_pandas()
        except:
            return pd.DataFrame()

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    mtab1, mtab2, mtab3, mtab4 = st.tabs([
        "📈 Route Performance",
        "🌤️ Weather Impact",
        "⚠️ Alerts",
        "🗺️ Station Map",
    ])

    # ── Route Performance ─────────────────────────────────────────────────────
    with mtab1:
        st.markdown("#### 📈 Weekly Events by Route")
        weekly_df = load_weekly_events()
        if not weekly_df.empty:
            routes = weekly_df["ROUTE_ID"].unique().tolist()
            selected_routes = st.multiselect("Select routes:", routes, default=routes[:3])
            if selected_routes:
                filtered = weekly_df[weekly_df["ROUTE_ID"].isin(selected_routes)]
                try:
                    pivot = filtered.pivot(index="WEEK_START_DATE", columns="ROUTE_ID", values="TOTAL_EVENTS")
                    st.line_chart(pivot)
                except:
                    st.bar_chart(filtered.set_index("WEEK_START_DATE")["TOTAL_EVENTS"])
        else:
            st.info("Weekly events data unavailable.")

        st.divider()
        st.markdown("#### 📅 Events by Day of Week")
        dow_df = load_dow()
        if not dow_df.empty:
            st.bar_chart(dow_df.set_index("DAY_OF_WEEK")["AVG_EVENTS"])
        else:
            st.info("Day of week data unavailable.")

        st.divider()
        st.markdown("#### 🏆 Route Reliability")
        rel_df = load_reliability()
        if not rel_df.empty:
            st.dataframe(rel_df, use_container_width=True)
            st.bar_chart(rel_df.set_index("ROUTE_ID")["RELIABILITY_PCT"])
        else:
            st.info("Reliability data unavailable.")

    # ── Weather Impact ────────────────────────────────────────────────────────
    with mtab2:
        st.markdown("#### 🌤️ Weather Condition vs MBTA Performance")
        weather_df = load_weather()
        if not weather_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Avg Events by Weather**")
                st.bar_chart(weather_df.set_index("WEATHER_CONDITION")["AVG_EVENTS"])
            with col2:
                st.markdown("**Avg Trips by Weather**")
                st.bar_chart(weather_df.set_index("WEATHER_CONDITION")["AVG_TRIPS"])
            st.divider()
            st.dataframe(weather_df, use_container_width=True)
        else:
            st.info("Weather data unavailable.")

        st.divider()
        st.markdown("#### 🌡️ Temperature vs Events")
        temp_df = load_temp()
        if not temp_df.empty:
            st.bar_chart(temp_df.set_index("TEMP_CATEGORY")["AVG_EVENTS"])
        else:
            st.info("Temperature data unavailable.")

    # ── Alerts ────────────────────────────────────────────────────────────────
    with mtab3:
        st.markdown("#### ⚠️ Alerts by Route")
        alerts_df = load_alerts()
        if not alerts_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Total Alerts by Route**")
                by_route = alerts_df.groupby("ROUTE_ID")["TOTAL_ALERTS"].sum().reset_index()
                st.bar_chart(by_route.set_index("ROUTE_ID")["TOTAL_ALERTS"])
            with col2:
                st.markdown("**Alerts by Cause**")
                by_cause = alerts_df.groupby("CAUSE")["TOTAL_ALERTS"].sum().reset_index()
                st.bar_chart(by_cause.set_index("CAUSE")["TOTAL_ALERTS"])
            st.divider()
            st.dataframe(alerts_df, use_container_width=True)
        else:
            st.info("Alerts data unavailable.")

        st.divider()
        st.markdown("#### 🚨 Top Anomalies Detected")
        anomaly_df = load_anomalies()
        if not anomaly_df.empty:
            st.dataframe(anomaly_df, use_container_width=True)
        else:
            st.info("Anomaly data unavailable.")

    # ── Station Map ───────────────────────────────────────────────────────────
    with mtab4:
        st.markdown("#### 🗺️ MBTA Station Map")
        import pydeck as pdk

        station_df = load_stations()
        if not station_df.empty:
            line_options = ["All Lines"] + sorted(station_df["LINE_NAME"].dropna().unique().tolist())
            selected_line = st.selectbox("Select Line:", line_options, key="mbta_line")

            map_stations = station_df.copy()
            if selected_line != "All Lines":
                map_stations = map_stations[map_stations["LINE_NAME"] == selected_line]

            color_map = {
                "Red Line":    [220, 50,  50,  200],
                "Blue Line":   [50,  50,  220, 200],
                "Orange Line": [255, 140, 0,   200],
                "Green Line":  [50,  180, 50,  200],
                "Silver Line": [150, 150, 150, 200],
            }
            map_stations = map_stations.copy()
            map_stations["COLOR"] = map_stations["LINE_NAME"].map(
                lambda x: color_map.get(x, [150, 150, 150, 200])
            )
            map_stations["SIZE"] = 150

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_stations,
                get_position=["LONGITUDE", "LATITUDE"],
                get_color="COLOR",
                get_radius="SIZE",
                pickable=True,
                auto_highlight=True,
            )

            view = pdk.ViewState(latitude=42.36, longitude=-71.06, zoom=11, pitch=0)

            tooltip = {
                "html": """
                    <b>{STATION_NAME}</b><br/>
                    🚇 {LINE_NAME}<br/>
                    📍 {MUNICIPALITY}<br/>
                    🔢 Events: {TOTAL_EVENTS}<br/>
                    🚌 Trips: {UNIQUE_TRIPS}
                """,
                "style": {
                    "backgroundColor": "#1a1a1a",
                    "color": "white",
                    "fontSize": "13px",
                    "padding": "10px",
                    "borderRadius": "8px",
                }
            }

            st.pydeck_chart(pdk.Deck(
                layers=[layer],
                initial_view_state=view,
                tooltip=tooltip,
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            ))

            st.markdown("🔴 Red &nbsp;&nbsp; 🔵 Blue &nbsp;&nbsp; 🟠 Orange &nbsp;&nbsp; 🟢 Green &nbsp;&nbsp; ⬜ Silver")

            st.divider()
            st.markdown("#### 📊 Station Details")
            st.dataframe(
                station_df[["STATION_NAME","LINE_NAME","MUNICIPALITY","TOTAL_EVENTS","UNIQUE_TRIPS","DAYS_WITH_SERVICE"]].sort_values("TOTAL_EVENTS", ascending=False),
                use_container_width=True
            )
        else:
            st.info("Station data unavailable. Check that CITYLENS_CORE.DIM_STATION and FACT_EVENTS tables exist.")