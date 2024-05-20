# Import python packages
import streamlit as st
import pandas as pd
import json
import os

#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

st.set_page_config(
    page_title="Snowflake LLM Hackathon V2",
    page_icon="📊",
    layout = "wide",
)

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


# ---------------------------------------
def get_classification(session,sql_body):
    prompt2 = f"""
        You are a SQL translator expert. Your primary job is to identify if a type of SQL statement is DDL or DML or DCL where SQL
        given between <context> and </context> tags. 
        Identify if it is DML, DDL or DCL and what kind of database object is created/altered. 
        Generate JSON output with key/value 
            stmt_type = DDL or DML
            object_type = table or view or function
            operation_type = create or alter
            schema_name = dbo or sales
            object_name =table or view or trigger name
        <context>
        {sql_body}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt2]).collect()
    return df_response
# ---------------------------------------
# Write directly to the app
st.title("Snowflake LLM V5 (18/5 04PM)")


#step-2 extract the sql script
sql_script_query = """
SELECT 
    metadata$filename as _stg_sql_file_name,
    metadata$FILE_LAST_MODIFIED::text as _stg_sql_file_load_ts,
    metadata$FILE_CONTENT_KEY as _stg_sql_file_md5,
    t.$1 as sql_script
FROM 
    @my_sql (file_format => 'myformat', pattern=>'.*MS-SQL-Server-Script.*[.]sql') t;
"""

sql_script_query_df = session.sql(sql_script_query).collect()

new_df = pd.DataFrame(sql_script_query_df,columns=['SQL_File_Name','Load_Time','MD5','SQL_Body'])

for index, row in new_df.iterrows():
    file_name = row['SQL_File_Name'].split('/')[-1]
    load_time = row['Load_Time']
    md5 = row['MD5']
    script = row['SQL_Body']
    st.subheader(file_name)
    
    col1, col2 = st.columns(2)
    with col1:  
        st.code(script, language='sql' , line_numbers = True)
    with col2:
        snowpark_row = get_classification(session,script)
        for element in snowpark_row:
            st.write(element.as_dict())
            json_body = element.as_dict()
            #st.code(json_body, language='json' , line_numbers = False)
            nested_json_string = json_body['RESPONSE']
            
            # Step 2: Parse the nested JSON string into a dictionary
            nested_json_object = json.loads(nested_json_string)
            #st.code(str(nested_json_object))
            # Step 3: Access individual elements
            stmt_type_val = nested_json_object['stmt_type']
            object_type_val = nested_json_object['object_type']
            operation_type_val = nested_json_object['operation_type']
            schema_name_val = nested_json_object['schema_name']
            object_name_val = nested_json_object['object_name']
            
            # Print the extracted elements
            #st.markdown(stmt_type_val)
            #st.markdown(object_type_val)
            #st.markdown(operation_type_val)
            #st.markdown(schema_name_val)
            #st.markdown(object_name_val)

            insert_sql = f"""
                insert into migration_script 
                (   sql_file_name,
                    sql_body,
                    _stg_sql_file_name,
                    _stg_sql_file_load_ts,
                    _stg_sql_file_md5,
                    cortex_worked,
                    cortex_status,
                    cortex_err_msg,
                    cortex_response,
                    stmt_type,
                    object_type,
                    operation_type,
                    schema_name,
                    object_name
                )
                values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """

            param_vals = [
                            file_name,
                            script,
                            row['SQL_File_Name'],
                            load_time,
                            md5,
                            'Yes',
                            'Completed',
                            'None',
                            str(json_body),
                            stmt_type_val,
                            object_type_val,
                            operation_type_val,
                            schema_name_val,
                            object_name_val
                        ]

            #insert into migration_script (sql_file_name,sql_body,_stg_sql_file_name,_stg_sql_file_load_ts,_stg_sql_file_md5) values (?,?,?,?,?)

            session.sql(insert_sql, params=param_vals).collect()

            break






