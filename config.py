import re
import streamlit as st
from snowflake.snowpark import Session
from snowflake_config import SNOWFLAKE_CONN

DB = "CITYLENS_MERGED_DB"

DISTRICT_MAP = {
    "A1": "Downtown", "A7": "East Boston", "A15": "Charlestown",
    "B2": "Roxbury", "B3": "Mattapan", "C6": "South Boston",
    "C11": "Dorchester", "D4": "South End", "D14": "Brighton/Allston",
    "E5": "West Roxbury", "E13": "Jamaica Plain", "E18": "Hyde Park"
}

NBHD_TO_DISTRICT = {
    "DOWNTOWN": "A1", "BEACON HILL": "A1", "WEST END": "A1",
    "NORTH END": "A1", "CHINATOWN": "A1", "LEATHER DISTRICT": "A1",
    "EAST BOSTON": "A7", "CHARLESTOWN": "A15",
    "ROXBURY": "B2", "MATTAPAN": "B3",
    "SOUTH BOSTON": "C6", "SOUTH BOSTON WATERFRONT": "C6",
    "DORCHESTER": "C11",
    "SOUTH END": "D4", "BACK BAY": "D4", "BAY VILLAGE": "D4",
    "FENWAY": "D4", "LONGWOOD": "D4",
    "ALLSTON": "D14", "BRIGHTON": "D14", "BRIGHTON/ALLSTON": "D14",
    "WEST ROXBURY": "E5", "ROSLINDALE": "E5",
    "JAMAICA PLAIN": "E13", "MISSION HILL": "E13",
    "HYDE PARK": "E18",
}
# Add DISTRICT_MAP names too
for code, name in DISTRICT_MAP.items():
    NBHD_TO_DISTRICT[name.upper()] = code

def replace_districts(text):
    for code, name in DISTRICT_MAP.items():
        text = re.sub(rf"\b{code}\b", f"{name} ({code})", text)
    return text

@st.cache_resource
def get_session():
    return Session.builder.configs(SNOWFLAKE_CONN).create()