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
                    "sub_questions":    [],
                    "use_multistep":    False,
                }

                result = citylens_graph.invoke(initial_state)

                branch        = result.get("branch", "cross")
                intent        = result.get("intent", "general")
                latency       = result.get("latency_ms", 0)
                score         = result.get("reflection_score", 0)
                total_ret     = result.get("total_retrievals", 0)
                answer        = result.get("final_answer", "No answer returned.")
                sub_questions = result.get("sub_questions", [])
                use_multistep = result.get("use_multistep", False)
                answer        = replace_districts(answer)

                # Save to conversation
                st.session_state.conversation.append({"role": "user", "content": query})
                st.session_state.conversation.append({
                    "role": "assistant", "content": answer,
                    "branch": branch, "intent": intent,
                    "latency": latency, "score": score, "total_ret": total_ret,
                    "sub_questions": sub_questions, "use_multistep": use_multistep,
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
        turn_num = 0
        for msg in st.session_state.conversation:
            if msg["role"] == "user":
                turn_num += 1
                st.markdown(f"""
                <div style='display:flex; justify-content:flex-end; margin-bottom:0.5rem; margin-top:1rem;'>
                    <div style='background:#1a1a1a; color:#ffffff; border-radius:12px 12px 2px 12px;
                                padding:0.6rem 1rem; max-width:75%; font-size:0.92rem;'>
                        {msg["content"]}
                    </div>
                </div>
                """, unsafe_allow_html=True)

            else:
                branch        = msg.get("branch", "cross")
                intent        = msg.get("intent", "general")
                latency       = msg.get("latency", 0)
                score         = msg.get("score", 0)
                total_ret     = msg.get("total_ret", 0)
                sub_questions = msg.get("sub_questions", [])
                use_multistep = msg.get("use_multistep", False)

                tag_label, tag_cls = tag_map.get(branch, ("◆ " + branch, "tag-cross"))

                # ── Thinking chain ────────────────────────────────────────────
                if use_multistep and sub_questions:
                    with st.expander(f"🟢 Thinking — broke into {len(sub_questions)} sub-questions", expanded=False):
                        for j, sq in enumerate(sub_questions):
                            st.markdown(f"""
                            <div style='display:flex; align-items:flex-start; gap:10px; margin-bottom:8px;'>
                                <div style='min-width:24px; height:24px; border-radius:50%;
                                            border:1.5px solid #22c55e; display:flex; align-items:center;
                                            justify-content:center; font-size:0.75rem; color:#22c55e;
                                            font-weight:600; flex-shrink:0;'>{j+1}</div>
                                <div style='font-size:0.88rem; color:#3a3a3a; padding-top:3px;'>{sq}</div>
                            </div>
                            """, unsafe_allow_html=True)

                # ── Answer card ───────────────────────────────────────────────
                answer_escaped = msg["content"].replace("$", "\\$").replace("\n", "<br>")
                st.markdown(
                    f"<div class='answer-card'>{answer_escaped}</div>",
                    unsafe_allow_html=True
                )

                # ── Metadata tags ─────────────────────────────────────────────
                confidence = round(score / 100, 2)

                # Build optional tags separately to avoid f-string nesting issues
                optional_tags = ""
                if use_multistep:
                    optional_tags += "<span style='background:#eff6ff; color:#1e40af; border:1px solid #bfdbfe; border-radius:99px; padding:3px 10px; font-size:0.72rem;'>Multi-step</span>"
                if turn_num > 1:
                    turns_label = f"Memory: {turn_num - 1} turn" + ("s" if turn_num - 1 != 1 else "")
                    optional_tags += f"<span style='background:#f7f6f2; color:#6b7280; border:1px solid #e8e6e0; border-radius:99px; padding:3px 10px; font-size:0.72rem;'>{turns_label}</span>"

                st.markdown(f"""
                <div style='display:flex; flex-wrap:wrap; gap:6px; margin-top:0.6rem; margin-bottom:1.5rem;'>
                    <span class='branch-tag {tag_cls}' style='margin:0;'>{tag_label}</span>
                    <span style='background:#f0fdf4; color:#166534; border:1px solid #bbf7d0;
                                 border-radius:99px; padding:3px 10px; font-size:0.72rem; font-weight:500;'>Confidence {confidence}</span>
                    <span style='background:#f7f6f2; color:#6b7280; border:1px solid #e8e6e0;
                                 border-radius:99px; padding:3px 10px; font-size:0.72rem;'>{total_ret} sources</span>
                    <span style='background:#f7f6f2; color:#6b7280; border:1px solid #e8e6e0;
                                 border-radius:99px; padding:3px 10px; font-size:0.72rem;'>{latency}ms</span>
                    {optional_tags}
                </div>
                """, unsafe_allow_html=True)

        # Scroll to latest answer
        st.markdown("<div id='latest-answer'></div>", unsafe_allow_html=True)
        st.components.v1.html("""
            <script>
                const el = window.parent.document.getElementById('latest-answer');
                if (el) el.scrollIntoView({behavior: 'smooth', block: 'end'});
            </script>
        """, height=0)