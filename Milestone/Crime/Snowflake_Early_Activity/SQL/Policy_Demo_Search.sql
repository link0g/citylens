
-- =====================================================
-- UNIFIED RAG SYSTEM: Policy + Crime Data
-- =====================================================
WITH params AS (
    -- SELECT 'What are the crime trends and policy responses in Boston?' AS QUESTION
    SELECT 'Which 2 districts have the most violent crime?' AS QUESTION
    -- SELECT 'How is Boston addressing gun violence?' AS QUESTION
),

query_embedding AS (
    SELECT 
        QUESTION,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', QUESTION) AS Q_VEC
    FROM params
),

policy_results AS (
    SELECT
        'POLICY' AS SOURCE_TYPE,
        p.POLICY_ID AS DOC_ID,
        p.SOURCE_FILE AS SOURCE_NAME,
        p.CHUNK_TEXT AS CONTENT,
        VECTOR_COSINE_SIMILARITY(p.EMBEDDING, q.Q_VEC) AS SCORE,
        q.QUESTION
    FROM DAMG7374_CRIME_DATE.PUBLIC.POLICY_DOCUMENTS p, query_embedding q
),

crime_results AS (
    SELECT
        'CRIME_DATA' AS SOURCE_TYPE,
        c.SUMMARY_ID AS DOC_ID,
        c.SUMMARY_TYPE || ': ' || c.DIMENSION_VALUE AS SOURCE_NAME,
        c.SUMMARY_TEXT AS CONTENT,
        VECTOR_COSINE_SIMILARITY(c.EMBEDDING, q.Q_VEC) AS SCORE,
        q.QUESTION
    FROM DAMG7374_CRIME_DATE.PUBLIC.CRIME_SUMMARIES c, query_embedding q
),

unified_results AS (
    SELECT * FROM policy_results
    UNION ALL
    SELECT * FROM crime_results
),

top_chunks AS (
    SELECT *
    FROM unified_results
    ORDER BY SCORE DESC
    LIMIT 8
),

combined_context AS (
    SELECT 
        LISTAGG('[' || SOURCE_TYPE || ' - ' || SOURCE_NAME || ']: ' || CONTENT, '\n\n') AS FULL_CONTEXT,
        MAX(QUESTION) AS QUESTION
    FROM top_chunks
)

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'You are an expert assistant on Boston crime data and city policies. 
Answer the question using BOTH crime statistics AND policy documents when relevant.
Be specific with numbers and cite sources.

Question: ' || QUESTION || '

Context (from crime data and policy documents):
' || FULL_CONTEXT
) AS AI_RESPONSE
FROM combined_context;
