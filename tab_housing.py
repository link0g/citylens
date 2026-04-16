import json
import pandas as pd
import streamlit as st
import pydeck as pdk
from config import DB, NBHD_TO_DISTRICT


def render_housing_tab(session, DB):
    st.markdown("### 🏠 Boston Housing Dashboard")

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
    def load_property_summary():
        return session.sql(f"""
            SELECT
                n.NEIGHBORHOOD_NAME,
                pt.LAND_USE_DESC AS PROPERTY_TYPE,
                pt.CATEGORY,
                COUNT(f.PROPERTY_ID)          AS TOTAL_PROPERTIES,
                ROUND(AVG(f.TOTAL_VALUE), 0)  AS AVG_VALUE,
                ROUND(AVG(f.PRICE_PER_SQFT), 0) AS AVG_PRICE_PER_SQFT,
                ROUND(AVG(f.LIVING_AREA), 0)  AS AVG_LIVING_AREA,
                ROUND(AVG(f.BEDROOMS), 1)     AS AVG_BEDROOMS
            FROM {DB}.HOUSING_CORE.FACT_PROPERTY_VALUE f
            JOIN {DB}.HOUSING_CORE.DIM_NEIGHBORHOOD n
                ON f.NEIGHBORHOOD_KEY = n.NEIGHBORHOOD_KEY
            JOIN {DB}.HOUSING_CORE.DIM_PROPERTY_TYPE pt
                ON f.PROPERTY_TYPE_KEY = pt.PROPERTY_TYPE_KEY
            WHERE pt.CATEGORY = 'residential'
            GROUP BY 1, 2, 3
            ORDER BY AVG_VALUE DESC
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
    nbhd_df      = load_neighborhood_summary()
    exc_df       = load_exceptions()
    prop_df      = load_property_summary()

    # ── KPIs ──────────────────────────────────────────────────────────────────
    total_props  = prop_df["TOTAL_PROPERTIES"].sum() if not prop_df.empty else 0
    avg_value    = prop_df["AVG_VALUE"].mean() if not prop_df.empty else 0
    avg_ppsqft   = prop_df["AVG_PRICE_PER_SQFT"].mean() if not prop_df.empty else 0
    n_nbhds      = nbhd_df["NEIGHBORHOOD"].nunique() if not nbhd_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Properties",    f"{int(total_props):,}")
    c2.metric("Avg Property Value",  f"${int(avg_value):,}")
    c3.metric("Avg Price / SqFt",    f"${int(avg_ppsqft):,}")
    c4.metric("Neighborhoods",       f"{n_nbhds}")

    st.divider()

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    htab1, htab2, htab3, = st.tabs([
        "📊 Neighborhood Analysis",
        "🏗️ Property Types",
        "🗺️ Price Map",
        
    ])

    # ── Tab 1: Neighborhood Analysis ─────────────────────────────────────────
    with htab1:
        if not nbhd_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### 🏆 Neighborhood Value Score Ranking")
                st.bar_chart(
                    nbhd_df.set_index("NEIGHBORHOOD")["VALUE_SCORE"].sort_values(ascending=False)
                )

            with col2:
                st.markdown("#### 🏷️ Tier Distribution")
                tier_counts = nbhd_df["NEIGHBORHOOD_TIER"].value_counts().reset_index()
                tier_counts.columns = ["Tier", "Count"]
                st.bar_chart(tier_counts.set_index("Tier")["Count"])

            st.divider()
            st.markdown("#### 💰 Most & Least Expensive Neighborhoods")

            if not exc_df.empty:
                expensive = exc_df[exc_df["EXCEPTION_TYPE"] == "TOP_5_MOST_EXPENSIVE"].copy()
                affordable = exc_df[exc_df["EXCEPTION_TYPE"] == "BOTTOM_5_LEAST_EXPENSIVE"].copy()

                col3, col4 = st.columns(2)
                with col3:
                    st.markdown("**🔴 Top 5 Most Expensive**")
                    if not expensive.empty:
                        for _, row in expensive.iterrows():
                            st.markdown(f"""
                            <div style='background:#fff1f2; border:1px solid #fecdd3; border-radius:10px;
                                        padding:0.7rem 1rem; margin-bottom:8px;'>
                                <div style='font-weight:600; color:#9f1239;'>{row['NEIGHBORHOOD_NAME']}</div>
                                <div style='font-size:0.85rem; color:#6b7280;'>
                                    Avg: <b>${int(row['AVG_PROPERTY_VALUE']):,}</b> &nbsp;|&nbsp;
                                    Median: <b>${int(row['MEDIAN_PROPERTY_VALUE']):,}</b> &nbsp;|&nbsp;
                                    $/sqft: <b>${int(row['AVG_PRICE_PER_SQFT']):,}</b>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

                with col4:
                    st.markdown("**🟢 Top 5 Most Affordable**")
                    if not affordable.empty:
                        for _, row in affordable.iterrows():
                            st.markdown(f"""
                            <div style='background:#ecfdf5; border:1px solid #a7f3d0; border-radius:10px;
                                        padding:0.7rem 1rem; margin-bottom:8px;'>
                                <div style='font-weight:600; color:#065f46;'>{row['NEIGHBORHOOD_NAME']}</div>
                                <div style='font-size:0.85rem; color:#6b7280;'>
                                    Avg: <b>${int(row['AVG_PROPERTY_VALUE']):,}</b> &nbsp;|&nbsp;
                                    Median: <b>${int(row['MEDIAN_PROPERTY_VALUE']):,}</b> &nbsp;|&nbsp;
                                    $/sqft: <b>${int(row['AVG_PRICE_PER_SQFT']):,}</b>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

            st.divider()
            st.markdown("#### 📋 Full Neighborhood Scorecard")
            st.dataframe(
                nbhd_df[["NEIGHBORHOOD", "NEIGHBORHOOD_TIER", "VALUE_SCORE", "PRICE_DENSITY_SCORE"]].sort_values("VALUE_SCORE", ascending=False),
                use_container_width=True,
                hide_index=True,
            )

    # ── Tab 2: Property Types ─────────────────────────────────────────────────
    with htab2:
        if not prop_df.empty:
            # Filter selector
            prop_types = ["All"] + sorted(prop_df["PROPERTY_TYPE"].dropna().unique().tolist())
            selected_type = st.selectbox("Filter by Property Type:", prop_types)

            filtered_prop = prop_df.copy()
            if selected_type != "All":
                filtered_prop = filtered_prop[filtered_prop["PROPERTY_TYPE"] == selected_type]

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 💵 Avg Value by Property Type")
                type_summary = prop_df.groupby("PROPERTY_TYPE")["AVG_VALUE"].mean().sort_values(ascending=False)
                st.bar_chart(type_summary)

            with col2:
                st.markdown("#### 📐 Avg Price/SqFt by Property Type")
                sqft_summary = prop_df.groupby("PROPERTY_TYPE")["AVG_PRICE_PER_SQFT"].mean().sort_values(ascending=False)
                st.bar_chart(sqft_summary)

            st.divider()
            st.markdown("#### 📋 Property Details by Neighborhood")
            display_cols = ["NEIGHBORHOOD_NAME", "PROPERTY_TYPE", "TOTAL_PROPERTIES",
                           "AVG_VALUE", "AVG_PRICE_PER_SQFT", "AVG_LIVING_AREA", "AVG_BEDROOMS"]
            st.dataframe(
                filtered_prop[display_cols].sort_values("AVG_VALUE", ascending=False),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Property data unavailable.")

    # ── Tab 3: Price Map ──────────────────────────────────────────────────────
    with htab3:
        st.markdown("#### 🗺️ Neighborhood Price Heatmap")
        st.caption("Color intensity = average property value. Darker red = more expensive.")

        try:
            geo_rows = load_neighborhood_geojson()

            # Build value lookup from neighborhood summary
            value_lookup = {}
            if not nbhd_df.empty:
                for _, row in nbhd_df.iterrows():
                    value_lookup[row["NEIGHBORHOOD"].upper()] = float(row["VALUE_SCORE"])

            max_score = max(value_lookup.values()) if value_lookup else 1

            features = []
            for row in geo_rows:
                nbhd = row["NEIGHBORHOOD_NAME"]
                try:
                    geom = json.loads(row["GEOJSON"])
                except:
                    continue

                score = value_lookup.get(nbhd.upper(), 0)
                ratio = score / max_score if max_score > 0 else 0

                # Color: low = light green, high = deep red
                r_val = int(50  + ratio * 200)
                g_val = int(180 - ratio * 150)
                b_val = 80

                features.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "neighborhood": nbhd,
                        "value_score": round(score, 1),
                        "tier": value_lookup.get(nbhd.upper(), "—"),
                        "fill_color": [r_val, g_val, b_val, 180],
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

            st.pydeck_chart(pdk.Deck(
                layers=[geo_layer],
                initial_view_state=pdk.ViewState(latitude=42.33, longitude=-71.07, zoom=11, pitch=0),
                tooltip={
                    "html": "<b>{neighborhood}</b><br/>📊 Value Score: {value_score}",
                    "style": {
                        "backgroundColor": "#1a1a1a",
                        "color": "white",
                        "fontSize": "13px",
                        "padding": "10px",
                        "borderRadius": "8px",
                    }
                },
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            ))
            st.markdown("🟢 Lower value &nbsp;&nbsp; 🟡 Mid &nbsp;&nbsp; 🔴 Higher value")
            st.caption("Hover over a neighborhood to see its value score")

        except Exception as e:
            st.warning(f"Map unavailable: {e}")

    