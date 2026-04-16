import streamlit as st
import json
import _snowflake

st.set_page_config(page_title="Boston Crime & Policy Q&A")

st.title("Boston Crime & Policy Q&A")
st.caption("Ask questions about Boston crime data and city policies")

# ✅ 你的 Agent endpoint（不用改）
AGENT_ENDPOINT = "/api/v2/databases/DAMG7374_CRIME_DATE/schemas/PUBLIC/agents/CRIME_POLICY_AGENT:run"

# ===== Chat history =====
if "messages" not in st.session_state:
    st.session_state.messages = []

# ===== Display chat =====
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ===== User input =====
if question := st.chat_input("Ask about Boston crime data or city policies..."):

    # 保存用户消息
    st.session_state.messages.append({"role": "user", "content": question})

    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):

            # ✅ 关键修复：用 input，不是 messages
            payload = json.dumps({
                "input": question,
                "stream": False
            })

            # 调用 Snowflake Agent
            resp = _snowflake.send_snow_api_request(
                "POST",
                AGENT_ENDPOINT,
                {},
                {},
                payload,
                {},
                300000
            )

            response_text = ""
            debug_info = None

            if resp["status"] == 200:
                body = json.loads(resp["content"])
                debug_info = body  # debug用

                for item in body.get("content", []):
                    if item.get("type") == "text":
                        response_text += item.get("text", "")
                    elif item.get("type") == "tool_result":
                        # ✅ 显示SQL / tool返回结果
                        response_text += "\n\n" + str(item.get("content", ""))

            else:
                response_text = "❌ API request failed."

            if not response_text:
                response_text = "⚠️ No response generated. Please try again."

            st.markdown(response_text)

            # 保存 assistant 回复
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_text
            })

# ===== Sidebar =====
with st.sidebar:
    st.subheader("Example questions")

    st.markdown("""
- How many crimes in 2023?
- Compare crime between 2022 and 2023
- Which district has the most violent crime?
- What are Boston's policies on gun violence?
""")

    if st.button("Clear chat"):
        st.session_state.messages = []
        st.rerun()

    # ===== Debug =====
    debug_mode = st.checkbox("Show Debug Info")

    if debug_mode and "messages" in st.session_state:
        st.subheader("Debug Output (Last Response)")
        if 'debug_info' in locals() and debug_info:
            st.json(debug_info)