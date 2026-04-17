import re
import json
import numpy as np
import pandas as pd
import streamlit as st
import pydeck as pdk
from config import DB, NBHD_TO_DISTRICT


def extract_highlighted_neighborhoods(answer):
    """Extract only neighborhoods from the RECOMMENDED line at end of answer."""
    highlighted = set()
    for line in answer.upper().split("\n"):
        if line.strip().startswith("RECOMMENDED:"):
            names = line.replace("RECOMMENDED:", "").strip()
            for part in names.split(","):
                name = part.strip()
                if name in NBHD_TO_DISTRICT:
                    highlighted.add(name)
    return highlighted


def render_housing_tab(session, DB):
    st.markdown("### 🏠 Boston Housing")

    # ── Data loaders ──────────────────────────────────────────────────────────
    @st.cache_data(ttl=300)
    def load_neighborhood_summary():
        return session.sql(f"""
            SELECT ENTITY_NAME AS NEIGHBORHOOD, NEIGHBORHOOD_TIER,
                   VALUE_SCORE, PRICE_DENSITY_SCORE, SUMMARY_TEXT
            FROM {DB}.HOUSING_SERVING.SRV_NEIGHBORHOOD_SUMMARY
            ORDER BY VALUE_SCORE DESC
        """).to_pandas()

    @st.cache_data(ttl=300)
    def load_exceptions():
        return session.sql(f"""
            SELECT EXCEPTION_TYPE, NEIGHBORHOOD_NAME, TOTAL_PROPERTIES,
                   AVG_PROPERTY_VALUE, MEDIAN_PROPERTY_VALUE, AVG_PRICE_PER_SQFT, RANK
            FROM {DB}.HOUSING_MART.MART_TOP_HOUSING_EXCEPTIONS
            ORDER BY EXCEPTION_TYPE, RANK
        """).to_pandas()

    @st.cache_data(ttl=300)
    def load_neighborhood_geojson():
        return session.sql(f"""
            SELECT
                NEIGHBORHOOD_NAME,
                ST_ASGEOJSON(
                    ST_TRANSFORM(
                        TO_GEOMETRY(
                            '{{"type":"' || GEOMETRY_TYPE || '","coordinates":' || GEOMETRY_COORDINATES::STRING || '}}'
                        ),
                        2249, 4326
                    )
                )::STRING AS GEOJSON
            FROM {DB}.HOUSING_CORE.DIM_NEIGHBORHOOD
        """).collect()

    # ── Load data ─────────────────────────────────────────────────────────────
    nbhd_df = load_neighborhood_summary()
    exc_df  = load_exceptions()

    # ── AI session state ──────────────────────────────────────────────────────
    if "housing_ai_answer" not in st.session_state:
        st.session_state.housing_ai_answer = ""
    if "housing_ai_highlights" not in st.session_state:
        st.session_state.housing_ai_highlights = set()

    # ── KPIs ──────────────────────────────────────────────────────────────────
    if not exc_df.empty:
        expensive = exc_df[exc_df["EXCEPTION_TYPE"] == "TOP_5_MOST_EXPENSIVE"]
        affordable = exc_df[exc_df["EXCEPTION_TYPE"] == "BOTTOM_5_LEAST_EXPENSIVE"]
        most_exp   = expensive.iloc[0] if not expensive.empty else None
        most_aff   = affordable.iloc[0] if not affordable.empty else None
    else:
        most_exp = most_aff = None

    n_nbhds = nbhd_df["NEIGHBORHOOD"].nunique() if not nbhd_df.empty else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f"""
        <div style='background:#fff; border:1px solid #e8e6e0; border-radius:10px; padding:0.8rem 1rem;'>
            <div style='font-size:0.72rem; color:#9ca3af; text-transform:uppercase; letter-spacing:0.05em;'>Neighborhoods</div>
            <div style='font-size:1.2rem; font-weight:600; color:#1a1a1a; margin-top:2px;'>{n_nbhds}</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        val = most_exp["NEIGHBORHOOD_NAME"].title() if most_exp is not None else "—"
        st.markdown(f"""
        <div style='background:#fff; border:1px solid #e8e6e0; border-radius:10px; padding:0.8rem 1rem;'>
            <div style='font-size:0.72rem; color:#9ca3af; text-transform:uppercase; letter-spacing:0.05em;'>Most Expensive</div>
            <div style='font-size:1.2rem; font-weight:600; color:#9f1239; margin-top:2px;'>{val}</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        val = most_aff["NEIGHBORHOOD_NAME"].title() if most_aff is not None else "—"
        st.markdown(f"""
        <div style='background:#fff; border:1px solid #e8e6e0; border-radius:10px; padding:0.8rem 1rem;'>
            <div style='font-size:0.72rem; color:#9ca3af; text-transform:uppercase; letter-spacing:0.05em;'>Most Affordable</div>
            <div style='font-size:1.2rem; font-weight:600; color:#065f46; margin-top:2px;'>{val}</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    htab1, htab2 = st.tabs([
        "🗺️ Price Map",
        "💰 Neighborhood Prices",
    ])

    # ── Tab 1: Price Map + AI ─────────────────────────────────────────────────
    with htab1:
        st.markdown("#### 🤖 Ask about Boston housing")
        st.caption("Ask a question and the map will highlight the relevant neighborhoods.")

        ai_col, btn_col = st.columns([5, 1])
        with ai_col:
            housing_q = st.text_input(
                "housing_q_map", label_visibility="collapsed",
                placeholder="e.g. Where is the most affordable place to buy a home in Boston?",
                key="housing_ai_map_input"
            )
        with btn_col:
            housing_ask = st.button("Ask", key="housing_ai_map_btn", use_container_width=True)

        # Quick question buttons
        quick_qs = [
            "Where should I buy a home in Boston?",
            "Which neighborhoods are most affordable?",
            "What are the most expensive areas?",
            "Where is good value for money?",
        ]
        cols = st.columns(2)
        for i, q in enumerate(quick_qs):
            with cols[i % 2]:
                if st.button(q, key=f"hq_{i}", use_container_width=True):
                    st.session_state.housing_prefill_q = q

        if "housing_prefill_q" in st.session_state:
            housing_q = st.session_state.housing_prefill_q
            del st.session_state.housing_prefill_q
            housing_ask = True

        if housing_ask and housing_q.strip():
            with st.spinner("Analyzing Boston housing data…"):
                ctx_nbhd = nbhd_df[["NEIGHBORHOOD","NEIGHBORHOOD_TIER","VALUE_SCORE"]].to_csv(index=False) if not nbhd_df.empty else ""
                ctx_exc  = exc_df[["EXCEPTION_TYPE","NEIGHBORHOOD_NAME","AVG_PROPERTY_VALUE","MEDIAN_PROPERTY_VALUE"]].to_csv(index=False) if not exc_df.empty else ""

                prompt = f"""You are a friendly Boston real estate advisor helping everyday people find a home.
Answer in simple, clear language. Avoid technical jargon.
Use specific neighborhood names and prices.
Structure: direct answer first, then 2-3 key points.

At the very end of your response, add exactly this line (no extra text):
RECOMMENDED: neighborhood1, neighborhood2, neighborhood3

Only include neighborhoods you are actively recommending, NOT ones mentioned as too expensive or used as contrast examples.

Neighborhood data:
{ctx_nbhd}

Price data:
{ctx_exc}

Question: {housing_q}"""

                try:
                    result = session.sql(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-haiku-4-5', $${prompt}$$) AS ANSWER
                    """).collect()
                    answer = result[0]["ANSWER"]
                    # Remove the RECOMMENDED line from display
                    display_answer = "\n".join(
                        line for line in answer.split("\n")
                        if not line.strip().upper().startswith("RECOMMENDED:")
                    ).strip()
                    st.session_state.housing_ai_answer = display_answer
                    st.session_state.housing_ai_highlights = extract_highlighted_neighborhoods(answer)
                except Exception as e:
                    st.session_state.housing_ai_answer = f"Error: {e}"
                    st.session_state.housing_ai_highlights = set()

        if st.session_state.housing_ai_answer:
            answer_escaped = st.session_state.housing_ai_answer.replace("$", "\\$")
            st.markdown("<div style='background:#ffffff; border:1px solid #e8e6e0; border-radius:12px; padding:1.2rem 1.5rem; margin-top:0.8rem; font-size:0.95rem; line-height:1.8; color:#2d2d2d;'>", unsafe_allow_html=True)
            st.markdown(answer_escaped)
            st.markdown("</div>", unsafe_allow_html=True)
            if st.session_state.housing_ai_highlights:
                names = ", ".join(n.title() for n in st.session_state.housing_ai_highlights)
                st.info(f"🗺️ Highlighted below: {names}")
            if st.button("Clear", key="housing_ai_clear"):
                st.session_state.housing_ai_answer = ""
                st.session_state.housing_ai_highlights = set()
                st.rerun()

        st.divider()
        st.markdown("#### 🗺️ Boston Neighborhood Price Map")
        st.caption("Hover over any neighborhood to see its price level. Red = more expensive, green = more affordable.")

        try:
            geo_rows = load_neighborhood_geojson()

            value_lookup = {}
            if not nbhd_df.empty:
                for _, row in nbhd_df.iterrows():
                    value_lookup[row["NEIGHBORHOOD"].upper()] = float(row["VALUE_SCORE"])

            scores = list(value_lookup.values())
            p10 = np.percentile(scores, 10) if scores else 0
            p90 = np.percentile(scores, 90) if scores else 1

            tier_lookup = {}
            if not nbhd_df.empty:
                for _, row in nbhd_df.iterrows():
                    tier_lookup[row["NEIGHBORHOOD"].upper()] = row["NEIGHBORHOOD_TIER"].title()

            ai_highlights = st.session_state.get("housing_ai_highlights", set())

            features = []
            for row in geo_rows:
                nbhd = row["NEIGHBORHOOD_NAME"]
                try:
                    geom = json.loads(row["GEOJSON"])
                except:
                    continue

                score = value_lookup.get(nbhd.upper(), 0)
                tier  = tier_lookup.get(nbhd.upper(), "—")
                ratio = (score - p10) / (p90 - p10) if (p90 - p10) > 0 else 0
                ratio = max(0.0, min(1.0, ratio))

                r_val = int(50  + ratio * 200)
                g_val = int(180 - ratio * 150)
                b_val = 80

                if ai_highlights:
                    alpha = 230 if nbhd.upper() in ai_highlights else 35
                    if nbhd.upper() in ai_highlights:
                        r_val = min(255, r_val + 40)
                else:
                    alpha = 180

                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "neighborhood": nbhd,
                        "tier": tier,
                        "fill_color": [r_val, g_val, b_val, alpha],
                    }
                })

            st.pydeck_chart(pdk.Deck(
                layers=[pdk.Layer(
                    "GeoJsonLayer",
                    data={"type": "FeatureCollection", "features": features},
                    get_fill_color="properties.fill_color",
                    get_line_color=[255, 255, 255, 200],
                    line_width_min_pixels=1,
                    pickable=True,
                    auto_highlight=True,
                )],
                initial_view_state=pdk.ViewState(latitude=42.33, longitude=-71.07, zoom=11, pitch=0),
                tooltip={
                    "html": "<b>{neighborhood}</b><br/>💰 {tier}",
                    "style": {
                        "backgroundColor": "#1a1a1a", "color": "white",
                        "fontSize": "13px", "padding": "10px", "borderRadius": "8px",
                    }
                },
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            ))
            st.markdown("🟢 Affordable &nbsp;&nbsp; 🟡 Mid-range &nbsp;&nbsp; 🔴 Expensive")

        except Exception as e:
            st.warning(f"Map unavailable: {e}")

    # ── Tab 2: Neighborhood Prices ────────────────────────────────────────────
    with htab2:
        if not exc_df.empty:
            expensive  = exc_df[exc_df["EXCEPTION_TYPE"] == "TOP_5_MOST_EXPENSIVE"].copy()
            affordable = exc_df[exc_df["EXCEPTION_TYPE"] == "BOTTOM_5_LEAST_EXPENSIVE"].copy()

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🔴 Most Expensive")
                for _, row in expensive.iterrows():
                    st.markdown(f"""
                    <div style='background:#fff1f2; border:1px solid #fecdd3; border-radius:10px;
                                padding:0.8rem 1rem; margin-bottom:8px;'>
                        <div style='font-weight:600; color:#9f1239; font-size:0.95rem;'>
                            {row['NEIGHBORHOOD_NAME']}
                        </div>
                        <div style='font-size:0.85rem; color:#6b7280; margin-top:4px;'>
                            Avg price: <b>${int(row['AVG_PROPERTY_VALUE']):,}</b><br/>
                            Median: <b>${int(row['MEDIAN_PROPERTY_VALUE']):,}</b>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

            with col2:
                st.markdown("#### 🟢 Most Affordable")
                for _, row in affordable.iterrows():
                    st.markdown(f"""
                    <div style='background:#ecfdf5; border:1px solid #a7f3d0; border-radius:10px;
                                padding:0.8rem 1rem; margin-bottom:8px;'>
                        <div style='font-weight:600; color:#065f46; font-size:0.95rem;'>
                            {row['NEIGHBORHOOD_NAME']}
                        </div>
                        <div style='font-size:0.85rem; color:#6b7280; margin-top:4px;'>
                            Avg price: <b>${int(row['AVG_PROPERTY_VALUE']):,}</b><br/>
                            Median: <b>${int(row['MEDIAN_PROPERTY_VALUE']):,}</b>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

            st.divider()

            # Simple bar chart — avg price by neighborhood
            if not nbhd_df.empty:
                st.markdown("#### 📊 All Neighborhoods by Price Level")
                st.caption("Based on relative value score — higher = more expensive")
                st.bar_chart(
                    nbhd_df.set_index("NEIGHBORHOOD")["VALUE_SCORE"].sort_values(ascending=False)
                )
        else:
            st.info("Housing data unavailable.")