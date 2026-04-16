import uuid
import streamlit as st
from datetime import datetime
from config import replace_districts, DISTRICT_MAP


def render_ai_tab(session, DB):
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
                answer_escaped = answer.replace("$", "\\$")
                st.markdown(answer_escaped)
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
                st.error("⚠️ Cannot import `citylens_langgraph`. Make sure it's in the same folder.")
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