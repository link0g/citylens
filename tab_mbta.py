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

        rel_df    = load_reliability()
        weekly_df = load_weekly_events()
        dow_df    = load_dow()

        # ── KPIs ─────────────────────────────────────────────────────────────
        if not rel_df.empty:
            best  = rel_df.iloc[0]
            worst = rel_df.iloc[-1]
            avg_rel = round(rel_df["RELIABILITY_PCT"].mean(), 1)
            c1, c2, c3 = st.columns(3)
            c1.metric("Most Reliable", best["ROUTE_ID"], f"{best['RELIABILITY_PCT']}%")
            c2.metric("Avg Reliability", f"{avg_rel}%")
            c3.metric("Least Reliable", worst["ROUTE_ID"], f"{worst['RELIABILITY_PCT']}%")
            st.divider()

        # ── Weekly Events ─────────────────────────────────────────────────────
        st.markdown("#### 📈 Weekly Events by Route")
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

        # ── Day of Week sorted Mon-Sun ────────────────────────────────────────
        st.markdown("#### 📅 Events by Day of Week")
        if not dow_df.empty:
            day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            dow_df["DAY_OF_WEEK"] = pd.Categorical(
                dow_df["DAY_OF_WEEK"], categories=day_order, ordered=True
            )
            dow_df = dow_df.sort_values("DAY_OF_WEEK")
            st.bar_chart(dow_df.set_index("DAY_OF_WEEK")["AVG_EVENTS"])
        else:
            st.info("Day of week data unavailable.")

        st.divider()

        # ── Route Reliability (table only, cleaned labels) ────────────────────
        st.markdown("#### 🏆 Route Reliability")
        if not rel_df.empty:
            rel_display = rel_df.copy()
            rel_display["RELIABILITY_GRADE"] = (
                rel_display["RELIABILITY_GRADE"]
                .str.replace("_", " ")
                .str.title()
            )
            rel_display = rel_display.rename(columns={
                "ROUTE_ID":         "Route",
                "RELIABILITY_PCT":  "Reliability %",
                "RELIABILITY_GRADE":"Grade",
                "BAD_DAYS":         "Bad Days",
                "TOTAL_DAYS":       "Total Days",
            })
            st.dataframe(
                rel_display[["Route", "Reliability %", "Grade", "Bad Days", "Total Days"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Reliability data unavailable.")

    # ── Weather Impact ────────────────────────────────────────────────────────
    with mtab2:
        st.markdown("#### 🌤️ Weather Impact on MBTA Performance")

        weather_df = load_weather()
        temp_df    = load_temp()

        if not weather_df.empty:
            # Clean up labels
            weather_df = weather_df.copy()
            weather_df["WEATHER_CONDITION"] = (
                weather_df["WEATHER_CONDITION"].str.replace("_", " ").str.title()
            )

            # Find normal baseline for comparison
            normal_row = weather_df[weather_df["WEATHER_CONDITION"] == "Normal"]
            normal_events = int(normal_row["AVG_EVENTS"].values[0]) if not normal_row.empty else None
            normal_trips  = int(normal_row["AVG_TRIPS"].values[0])  if not normal_row.empty else None

            # KPI insight cards
            if normal_events:
                cols = st.columns(len(weather_df))
                for col, (_, row) in zip(cols, weather_df.iterrows()):
                    cond   = row["WEATHER_CONDITION"]
                    events = int(row["AVG_EVENTS"])
                    trips  = int(row["AVG_TRIPS"])
                    diff_pct = round((events - normal_events) / normal_events * 100, 1) if cond != "Normal" else 0
                    diff_str = f"{diff_pct:+.1f}% vs normal" if cond != "Normal" else "Baseline"
                    color = "#9f1239" if diff_pct < -2 else ("#065f46" if diff_pct > 2 else "#6b7280")
                    col.markdown(f"""
                    <div style='background:#fff; border:1px solid #e8e6e0; border-radius:10px;
                                padding:0.8rem 1rem; text-align:center;'>
                        <div style='font-size:0.8rem; color:#6b7280; margin-bottom:4px;'>{cond}</div>
                        <div style='font-size:1.2rem; font-weight:600; color:#1a1a1a;'>{events:,}</div>
                        <div style='font-size:0.75rem; color:#9ca3af;'>avg events</div>
                        <div style='font-size:0.78rem; font-weight:500; color:{color}; margin-top:4px;'>{diff_str}</div>
                    </div>
                    """, unsafe_allow_html=True)

            st.divider()

            # Charts side by side — no table
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Avg Events by Weather**")
                st.bar_chart(weather_df.set_index("WEATHER_CONDITION")["AVG_EVENTS"])
            with col2:
                st.markdown("**Avg Trips by Weather**")
                st.bar_chart(weather_df.set_index("WEATHER_CONDITION")["AVG_TRIPS"])

        else:
            st.info("Weather data unavailable.")

        st.divider()

        # Temperature section
        st.markdown("#### 🌡️ Temperature vs Events")
        if not temp_df.empty:
            temp_df = temp_df.copy()
            cold_val = temp_df[temp_df["TEMP_CATEGORY"] == "COLD"]["AVG_EVENTS"].values
            warm_val = temp_df[temp_df["TEMP_CATEGORY"] == "WARM"]["AVG_EVENTS"].values
            if len(cold_val) > 0 and len(warm_val) > 0:
                diff = round(abs(int(warm_val[0]) - int(cold_val[0])) / int(cold_val[0]) * 100, 1)
                st.info(f"🌡️ Temperature has minimal impact on MBTA ridership — cold vs warm days vary by less than **{diff}%** in average event volume. Weather condition (rain/snow) matters more than temperature alone.")

            with st.expander("Show temperature chart"):
                temp_order = ["COLD", "MILD", "WARM"]
                temp_df["TEMP_CATEGORY"] = pd.Categorical(
                    temp_df["TEMP_CATEGORY"], categories=temp_order, ordered=True
                )
                temp_df = temp_df.sort_values("TEMP_CATEGORY")
                temp_df["TEMP_CATEGORY"] = temp_df["TEMP_CATEGORY"].str.title()
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
            station_display = station_df.groupby(["STATION_NAME", "MUNICIPALITY"]).agg(
                Lines=("LINE_NAME", lambda x: ", ".join(sorted(x.dropna().unique()))),
                Total_Events=("TOTAL_EVENTS", "max"),
                Unique_Trips=("UNIQUE_TRIPS", "max"),
                Days_With_Service=("DAYS_WITH_SERVICE", "max"),
            ).reset_index().sort_values("Total_Events", ascending=False)

            station_display = station_display.rename(columns={
                "STATION_NAME":   "Station",
                "MUNICIPALITY":   "City",
            })
            st.dataframe(station_display, use_container_width=True, hide_index=True)
            
        else:
            st.info("Station data unavailable.")