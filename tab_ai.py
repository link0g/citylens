import uuid
import streamlit as st
from datetime import datetime
from config import replace_districts, DISTRICT_MAP


def render_ai_tab(session, DB):

    # ── Session state ─────────────────────────────────────────────────────────
    if "conversation" not in st.session_state:
        st.session_state.conversation = []
    if "input_counter" not in st.session_state:
        st.session_state.input_counter = 0

    tag_map = {
        "housing":        ("🏠 Housing",        "tag-housing"),
        "transportation": ("🚇 Transportation", "tag-transportation"),
        "crime":          ("🚨 Crime",          "tag-crime"),
        "cross":          ("🗺️ Cross-domain",   "tag-cross"),
    }

    # ── Input box (always at top) ─────────────────────────────────────────────
    col_q, col_btn = st.columns([6, 1])
    with col_q:
        query = st.text_input(
            "q",
            label_visibility="collapsed",
            placeholder="Ask about Boston housing, crime, or transit…",
            key=f"query_box_{st.session_state.input_counter}",
        )
    with col_btn:
        ask = st.button("Ask →", use_container_width=True)

    # Clear button
    if st.session_state.conversation:
        if st.button("🗑️ Clear conversation", key="clear_conv"):
            st.session_state.conversation = []
            st.session_state.history = []
            st.rerun()

    # Handle prefill from sidebar
    if st.session_state.prefill:
        query = st.session_state.prefill
        st.session_state.prefill = ""
        ask = True

    # ── Process question ──────────────────────────────────────────────────────
    if ask and query.strip():
        with st.spinner("Analyzing Boston data…"):
            try:
                from citylens_langgraph import citylens_graph

                # Build context from recent conversation (last 2 turns)
                if st.session_state.conversation:
                    context_lines = []
                    for msg in st.session_state.conversation[-4:]:
                        if msg["role"] == "user":
                            context_lines.append(f"User: {msg['content']}")
                        else:
                            # 只传简短摘要，不传完整回答
                            short = msg["content"][:200].replace("\n", " ")
                            context_lines.append(f"Assistant: {short}…")
                    context = "\n".join(context_lines)
                    enriched_query = f"Context from previous exchange:\n{context}\n\nCurrent question: {query}"
                else:
                    enriched_query = query

                initial_state = {
                    "user_query":       enriched_query,
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

                # Save to conversation
                st.session_state.conversation.append({"role": "user", "content": query})
                st.session_state.conversation.append({
                    "role": "assistant", "content": answer,
                    "branch": branch, "intent": intent,
                    "latency": latency, "score": score, "total_ret": total_ret,
                })

                # Save to sidebar history
                st.session_state.history.append({
                    "query": query, "branch": branch,
                    "latency": latency, "score": score,
                    "ts": datetime.now().strftime("%H:%M"),
                    "answer": answer,
                })
                st.session_state.input_counter += 1
                st.rerun()

            except ImportError:
                st.error("⚠️ Cannot import `citylens_langgraph`. Make sure it's in the same folder.")
            except Exception as e:
                st.error(f"⚠️ Error: {e}")

    elif ask:
        st.warning("Please enter a question.")

    # ── Empty state ───────────────────────────────────────────────────────────
    if not st.session_state.conversation:
        st.markdown("""
        <div class='empty-state'>
            <div class='empty-icon'>◎</div>
            <div class='empty-text'>Ask anything about Boston</div>
            <div class='empty-sub'>Housing · Crime · Transit · Neighborhoods</div>
        </div>
        """, unsafe_allow_html=True)

    # ── Conversation history ──────────────────────────────────────────────────
    else:
        st.divider()
        for msg in st.session_state.conversation:
            if msg["role"] == "user":
                st.markdown(f"""
                <div style='display:flex; justify-content:flex-end; margin-bottom:0.5rem; margin-top:1rem;'>
                    <div style='background:#1a1a1a; color:#ffffff; border-radius:12px 12px 2px 12px;
                                padding:0.6rem 1rem; max-width:75%; font-size:0.92rem;'>
                        {msg["content"]}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                branch    = msg.get("branch", "cross")
                intent    = msg.get("intent", "general")
                latency   = msg.get("latency", 0)
                score     = msg.get("score", 0)
                total_ret = msg.get("total_ret", 0)

                tag_label, tag_cls = tag_map.get(branch, ("◆ " + branch, "tag-cross"))

                st.markdown(f"""
                <div style='margin-top:0.5rem; margin-bottom:0.2rem;'>
                    <span class='branch-tag {tag_cls}'>{tag_label}</span>
                    <span class='branch-tag' style='background:#f7f6f2; color:#9ca3af; border:1px solid #e8e6e0;'>
                        {intent.replace("_", " ").title()}
                    </span>
                </div>
                """, unsafe_allow_html=True)

                answer_escaped = msg["content"].replace("$", "\\$").replace("\n", "<br>")
                st.markdown(
                    f"<div class='answer-card'>{answer_escaped}</div>",
                    unsafe_allow_html=True
                )

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
                <div style='margin-bottom:1rem;'></div>
                """, unsafe_allow_html=True)

        # Scroll to latest answer after rerun
        st.markdown("<div id='latest-answer'></div>", unsafe_allow_html=True)
        st.components.v1.html("""
            <script>
                const el = window.parent.document.getElementById('latest-answer');
                if (el) el.scrollIntoView({behavior: 'smooth', block: 'end'});
            </script>
        """, height=0)