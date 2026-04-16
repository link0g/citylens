import json
import pandas as pd
import streamlit as st
import pydeck as pdk
from datetime import datetime, timedelta
from config import DISTRICT_MAP, NBHD_TO_DISTRICT


def render_crime_tab(session, DB):
    st.markdown("### 🚨 Boston Crime Dashboard")

    @st.cache_data(ttl=300)
    def load_crime_data():
        return session.sql(f"""
            SELECT DISTRICT, LAT, LONG, OCCURRED_ON_DATE
            FROM {DB}.CRIME_PUBLIC.CRIME_RAW
            WHERE LAT IS NOT NULL AND LONG IS NOT NULL
            AND OCCURRED_ON_DATE >= DATEADD(year, -5, CURRENT_DATE)
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
            FROM CITYLENS_MERGED_DB.HOUSING_CORE.DIM_NEIGHBORHOOD
        """).collect()

    df = load_crime_data()

    if df.empty:
        st.warning("No crime data found.")
        return

    df["OCCURRED_ON_DATE"] = pd.to_datetime(df["OCCURRED_ON_DATE"])
    df["year"] = df["OCCURRED_ON_DATE"].dt.year

    # ── Filters ──────────────────────────────────────────────────────────────
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

    # ── KPIs ──────────────────────────────────────────────────────────────────
    agg = df_filtered["DISTRICT"].value_counts().reset_index()
    agg.columns = ["DISTRICT", "crime_count"]
    agg["district_name"] = agg["DISTRICT"].map(DISTRICT_MAP).fillna(agg["DISTRICT"])

    most = agg.iloc[0] if len(agg) > 0 else None
    agg_valid = agg[agg["DISTRICT"].isin(DISTRICT_MAP.keys())]
    safest = agg_valid.iloc[-1] if len(agg_valid) > 1 else most

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Incidents", f"{len(df_filtered):,}")
    c2.metric("🔴 Highest Risk", most["district_name"] if most is not None else "—")
    c3.metric("🟢 Lowest Risk",  safest["district_name"] if safest is not None else "—")

    st.divider()

    # ── Risk level helpers ────────────────────────────────────────────────────
    counts = df_filtered["DISTRICT"].value_counts()
    q66 = counts.quantile(0.66)
    q33 = counts.quantile(0.33)

    def risk_level(d):
        c = counts.get(d, 0)
        return "High" if c > q66 else ("Medium" if c > q33 else "Low")

    # ── Map + Ranking ─────────────────────────────────────────────────────────
    left, right = st.columns([2, 1])

    with left:
        st.markdown("#### 🗺️ Crime Map")

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

        try:
            geo_rows = load_neighborhood_geojson()
            crime_counts = df_filtered["DISTRICT"].value_counts().to_dict()
            max_count = max(crime_counts.values()) if crime_counts else 1

            features = []
            for row in geo_rows:
                nbhd = row["NEIGHBORHOOD_NAME"]
                try:
                    geom = json.loads(row["GEOJSON"])
                except:
                    continue

                district_code = NBHD_TO_DISTRICT.get(nbhd.upper())
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

            st.pydeck_chart(pdk.Deck(
                layers=[geo_layer],
                initial_view_state=pdk.ViewState(latitude=42.33, longitude=-71.07, zoom=11, pitch=0),
                tooltip={
                    "html": "<b>{neighborhood}</b><br/>📍 District: {district}<br/>🚨 Incidents: {crime_count}",
                    "style": {"backgroundColor": "#1a1a1a", "color": "white",
                              "fontSize": "13px", "padding": "10px", "borderRadius": "8px"}
                },
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            ))
            st.markdown("🟢 Safer &nbsp;&nbsp; 🟡 Medium &nbsp;&nbsp; 🔴 Higher concern")
            st.caption("Hover over a neighborhood to see details")

        except Exception as e:
            st.warning(f"Map unavailable: {e}")

        map_df = df_filtered.rename(columns={"LAT": "lat", "LONG": "lon"})
        map_df = map_df.dropna(subset=["lat", "lon", "DISTRICT"]).copy()
        map_df["risk"] = map_df["DISTRICT"].apply(risk_level)
        if risk_filter != "All":
            map_df = map_df[map_df["risk"] == risk_filter]
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

    # ── Trend Chart ───────────────────────────────────────────────────────────
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