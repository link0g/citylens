import pandas as pd
import streamlit as st
import pydeck as pdk


def render_mbta_tab(session, DB):
    st.markdown("### 🚇 MBTA Transportation Dashboard")

    # ── Data loaders ──────────────────────────────────────────────────────────
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

            st.pydeck_chart(pdk.Deck(
                layers=[pdk.Layer(
                    "ScatterplotLayer",
                    data=map_stations,
                    get_position=["LONGITUDE", "LATITUDE"],
                    get_color="COLOR",
                    get_radius="SIZE",
                    pickable=True,
                    auto_highlight=True,
                )],
                initial_view_state=pdk.ViewState(latitude=42.36, longitude=-71.06, zoom=11, pitch=0),
                tooltip={
                    "html": "<b>{STATION_NAME}</b><br/>🚇 {LINE_NAME}<br/>📍 {MUNICIPALITY}<br/>🔢 Events: {TOTAL_EVENTS}<br/>🚌 Trips: {UNIQUE_TRIPS}",
                    "style": {"backgroundColor": "#1a1a1a", "color": "white",
                              "fontSize": "13px", "padding": "10px", "borderRadius": "8px"}
                },
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
            st.info("Station data unavailable.")