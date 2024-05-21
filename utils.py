from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import pandas as pd

import streamlit as st
import os

def get_snowpark_session() -> Session:
    # Step-1 Snowflake Connections
    connection_parameters = {
           "ACCOUNT":st.secrets["ACCOUNT"],
            "USER":st.secrets["USER"],
            "PASSWORD":st.secrets["PASSWORD"],
            "ROLE":st.secrets["ROLE"],
            "DATABASE":st.secrets["DATABASE"],
            "SCHEMA":st.secrets["SCHEMA"],
            "WAREHOUSE":st.secrets["WAREHOUSE"]
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

#if same query is passed, then it returns the same dataframe.
@st.cache_data
def get_df(query) -> pd.DataFrame:
    try:
        # Check if the query is a string
        if not isinstance(query, str):
            raise ValueError("Query must be a string")

        session = get_snowpark_session()
        df = session.sql(query).collect()
        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return pd.DataFrame()  
    
 
def get_single_column(query):
    lst = []
    sql_db_df = get_df(query)
    for row in sql_db_df:
        lst.append(row.as_dict().get('name'))
    return lst
