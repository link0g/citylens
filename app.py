import streamlit as st
import uuid
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CityLens Boston",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=Inter:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Hide only menu and footer, keep header for sidebar toggle */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* Make header transparent so it doesn't show as white bar */
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

/* Sample question buttons */
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

/* Ask button (last column) */
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

/* Text input */
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

/* Hero */
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

/* Branch tags */
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

/* Answer card */
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

/* Metrics */
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
.metric-label {
    font-size: 0.7rem;
    color: #9ca3af;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 3px;
}
.score-bar { height: 3px; background: #f0ede8; border-radius: 99px; margin-top: 8px; overflow: hidden; }
.score-fill { height: 100%; border-radius: 99px; background: #1a1a1a; }

/* History */
.hist-item { padding: 0.6rem 0; border-bottom: 1px solid #f0ede8; font-size: 0.8rem; }
.hist-q { color: #3a3a3a; font-weight: 500; margin-bottom: 2px; }
.hist-meta { color: #b0a89c; font-size: 0.72rem; }

/* Pipeline */
.pipe-item { display: flex; align-items: center; gap: 8px; font-size: 0.78rem; color: #9ca3af; padding: 4px 0; }
.pipe-dot { width: 6px; height: 6px; border-radius: 50%; background: #d1d5db; flex-shrink: 0; }

/* Divider */
.div-line { border-top: 1px solid #f0ede8; margin: 1.2rem 0; }

/* Sidebar label */
.sidebar-label {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #b0a89c;
    font-weight: 600;
    margin-bottom: 8px;
    margin-top: 4px;
}

/* Empty state */
.empty-state { text-align: center; padding: 4rem 0; }
.empty-icon { font-size: 2.5rem; margin-bottom: 1rem; }
.empty-text {
    font-family: 'Instrument Serif', serif;
    font-size: 1.4rem;
    color: #c8c4bc;
    font-weight: 400;
}
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
                "housing": "#065f46",
                "transportation": "#1e40af",
                "crime": "#9f1239",
                "cross": "#6b21a8"
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

# ── Main ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class='hero-wrap'>
    <div class='hero-title'>City<em>Lens</em></div>
    <div class='hero-sub'>Boston · Housing · Crime · Transit</div>
</div>
""", unsafe_allow_html=True)

# Prefill logic
default_val = st.session_state.prefill if st.session_state.prefill else ""

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

# ── Run query ─────────────────────────────────────────────────────────────────
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

            st.session_state.history.append({
                "query":   query,
                "branch":  branch,
                "latency": latency,
                "score":   score,
                "ts":      datetime.now().strftime("%H:%M"),
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

            formatted = answer.replace("\n", "<br>")
            st.markdown(f"<div class='answer-card'>{formatted}</div>", unsafe_allow_html=True)

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

# ── Empty state ───────────────────────────────────────────────────────────────
if not st.session_state.history and not (ask and query.strip()):
    st.markdown("""
    <div class='empty-state'>
        <div class='empty-icon'>🏙️</div>
        <div class='empty-text'>Ask anything about Boston</div>
        <div class='empty-sub'>Housing · Crime · Transit · Neighborhoods</div>
    </div>
    """, unsafe_allow_html=True)