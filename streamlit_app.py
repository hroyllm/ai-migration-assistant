import streamlit as st
import replicate
import os
from transformers import AutoTokenizer
from snowflake.snowpark import Session
import pandas as pd
import json

import utils
import helper
import helper_stats
import helper_table_ddl
import helper_function_ddl
import helper_view_ddl
import helper_trigger_ddl
import helper_constraints_ddl

# Set assistant icon to Snowflake logo
icons = {"assistant": "./Snowflake_Logomark_blue.svg", "user": "⛷️"}

st.set_page_config(
    page_title="Legacy To Snowflake Migration LLM App",
    layout = "wide",
    #initial_sidebar_state= 'collapsed',
    menu_items= {
        'Get Help':'https://docs.snowflake.com/',
        'About': 'Hiresh Roy'
    }
)

replicate_api = st.secrets["REPLICATE_API"]

# Create a sidebar with navigation links
st.sidebar.image('Snowflake_Logomark_blue.svg')
st.sidebar.title("AI Assistant For Data Migration")
st.sidebar.markdown("Build Using ❄️ Snowflake/Arctic LLM ❄️")



session = utils.get_snowpark_session()

# PART 1 - Side Navbar

tabs = ["Ask Arctic About Platform Migration", "All About Migration Lifecycle","Translate SQL Server to Snowflake"]
st.sidebar.divider()
current_tab = st.sidebar.radio("Choose Your Action", tabs)

st.sidebar.divider()
st.sidebar.markdown("""
                    Technology Used
                    - Streamlit/Python
                    - Snowflake-Arctic/Cortex
                    - Replicate.com API
                    """)

st.sidebar.divider()
st.sidebar.markdown("""
                    - Build By: Hiresh Roy
                    - May, 2024
                    """)



# following function is not going to be used.
def clear_chat_history():
    st.session_state.messages = [{"role": "assistant", "content": "Hi. I am Data Migration AI Assistant, build on the top of Snowflake/Arctic LLM Foundation Model.Ask me anything about Legacy to Modern Data Platform migration challenges."}]

@st.cache_resource(show_spinner=False)
def get_tokenizer():
    #print('I am called...<get_tokenizer> ')
    """Get a tokenizer to make sure we're not sending too much text
    to the Model. Eventually we will replace this with ArcticTokenizer
    """
    return AutoTokenizer.from_pretrained("huggyllama/llama-7b")

def get_num_tokens(prompt):
    #print('I am called...<get_num_tokens> ')
    #"""Get the number of tokens in a given prompt"""
    tokenizer = get_tokenizer()
    tokens = tokenizer.tokenize(prompt)
    print('The number of tokens'+ str(len(tokens)))
    return len(tokens)

# Function for generating Snowflake Arctic response
def generate_arctic_response():
    prompt = []
    for dict_message in st.session_state.messages:
        if dict_message["role"] == "user":
            prompt.append("<|im_start|>user\n" + dict_message["content"] + "<|im_end|>")
        else:
            prompt.append("<|im_start|>assistant\n" + dict_message["content"] + "<|im_end|>")
    
    prompt.append("<|im_start|>assistant")
    prompt.append("")
    prompt_str = "\n".join(prompt)
    
    if get_num_tokens(prompt_str) >= 3072:
        st.error("Conversation length too long. Please keep it under 3072 tokens.")
        st.button('Clear chat history', on_click=clear_chat_history, key="clear_chat_history")
        st.stop()

    for event in replicate.stream("snowflake/snowflake-arctic-instruct",
                           input={"prompt": prompt_str,
                                  "prompt_template": r"{prompt}",
                                  "temperature": 0,
                                  "top_p": 1,
                                  }):
        yield str(event)

if current_tab == "Ask Arctic About Platform Migration":
    st.markdown('<h1 style="color: #2596be;">Data Migration: Ask The AI Experts Anything</h1>', unsafe_allow_html=True)
    st.markdown('<h4 style="color: #2596be;">LLM Chatbot Build Using Snowflake/Arctic Foundational Model</h4>', unsafe_allow_html=True)
    st.divider()
    if "messages" not in st.session_state.keys():
        st.session_state.messages = [{"role": "assistant", "content": "Hi. I am Data Migration AI Assistant, build on the top of Snowflake/Arctic LLM Foundation Model.Ask me anything about Legacy to Modern Data Platform migration challenges."}]

    # Display or clear chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=icons[message["role"]]):
            st.write(message["content"])

    # User-provided prompt
    if prompt := st.chat_input(disabled=not replicate_api):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="⛷️"):
            st.write(prompt)


    # Generate a new response if last message is not from assistant
    if st.session_state.messages[-1]["role"] != "assistant":
        with st.chat_message("assistant", avatar="./Snowflake_Logomark_blue.svg"):
            response = generate_arctic_response()
            full_response = st.write_stream(response)
        message = {"role": "assistant", "content": full_response}
        st.session_state.messages.append(message)
if current_tab == "All About Migration Lifecycle":
    st.markdown('<h1 style="color: #2596be;">Data Platform Migration Lifecycle</h1>', unsafe_allow_html=True)
    st.markdown('<h4 style="color: #2596be;">MS SQL Server to Snowflake</h4>', unsafe_allow_html=True)
    st.divider()
    st.image('end-to-end-flow-v2.png')

# Global Objects
classification_done = False

if current_tab == "Translate SQL Server to Snowflake":
    st.markdown('<h1 style="color: #2596be;">Start Your SQL Server To Snowflake Migration Journey</h1>', unsafe_allow_html=True)
    st.markdown('<h5 style="color: #d45b90ff;">[MS SQL Server Script Exist In Snowflake Stage Location]</h5>', unsafe_allow_html=True)
    st.divider()

    #db_name, schema_name,stage_name,target_table = 'DEMO','PUBLIC','MY_SQL','MIGRATION_SCRIPT'

    #These are global variables
    db_name, schema_name,stage_name,target_table = '','','',''

    new_df = pd.DataFrame()

    st.markdown('<h4 style="color: #2596be;">Show Me Some Sample MS SQL Scripts</h4>', unsafe_allow_html=True)
    #load all the script with summary
    
    with st.expander("Expand/Collapse", expanded=True):
        if st.button('Show First 5 MS SQL Scripts - From Snowflake Stage Location'):
            sql_script_query = f"""
                                SELECT 
                                    metadata$filename as _stg_sql_file_name,
                                    metadata$FILE_LAST_MODIFIED::text as _stg_sql_file_load_ts,
                                    metadata$FILE_CONTENT_KEY as _stg_sql_file_md5,
                                    t.$1 as sql_script
                                FROM 
                                    @demo.public.my_sql (file_format => 'myformat', pattern=>'.*MS-SQL-Server-Script.*[.]sql') t
                                limit 5;
                                """
            sql_script_query_df = session.sql(sql_script_query).collect()

            new_df = pd.DataFrame(sql_script_query_df,columns=['SQL_File_Name','Load_Time','MD5','SQL_Body'])
            for index, row in new_df.iterrows():
                file_name = row['SQL_File_Name'].split('/')[-1]
                load_time = row['Load_Time']
                md5 = row['MD5']
                script = row['SQL_Body']
                #st.markdown(f"""<h6 style="color: #a5fecb;">{file_name}</h6>""", unsafe_allow_html=True)
                tab01, tab02 = st.tabs(['SQL Script','File Metadata'])
                with tab01:
                    st.code(script, language='sql' , line_numbers = True)
                with tab02:
                    st.markdown(f"""
                            - File Name: {file_name}
                            - File Load Time: {load_time}
                            - File MD5: {md5}
                            """)
    st.divider()
    st.markdown('<h4 style="color: #2596be;">Step-1 MS SQL Server - Object Classification</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL Script is passed to Snowflake cortex function for object classification and result comes back in JSON format</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collapse", expanded=True): 
        if st.button('Run Object Classification Task '):
            sql_script_query = f"""
                                    SELECT 
                                        metadata$filename as _stg_sql_file_name,
                                        metadata$FILE_LAST_MODIFIED::text as _stg_sql_file_load_ts,
                                        metadata$FILE_CONTENT_KEY as _stg_sql_file_md5,
                                        t.$1 as sql_script
                                    FROM 
                                        @demo.public.my_sql (file_format => 'myformat', pattern=>'.*MS-SQL-Server-Script.*[.]sql') t
                                    limit 50;
                                    """
            sql_script_query_df = session.sql(sql_script_query).collect()

            new_df = pd.DataFrame(sql_script_query_df,columns=['SQL_File_Name','Load_Time','MD5','SQL_Body'])
            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Table DDL</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)

            for index, row in new_df.iterrows():
                file_name = row['SQL_File_Name'].split('/')[-1]
                load_time = row['Load_Time']
                md5 = row['MD5']
                script = row['SQL_Body']
                
                
                col1, col2 = st.columns(2)
                with col1:  
                    tab01, tab02 = st.tabs(['MS SQL Script',file_name ])
                    with tab01:
                        st.code(script, language='sql' , line_numbers = True)
                    with tab02:
                        st.markdown(f"""
                            - File Name: {file_name}
                            - File Load Time: {load_time}
                            - File MD5: {md5}
                            """)
                with col2:
                    snowpark_row = helper.get_sql_object_classification_json(session,script)
                    for element in snowpark_row:
                        st.write(element.as_dict())
                        json_body = element.as_dict()
                        nested_json_string = json_body['RESPONSE']
                        try:
                            nested_json_object = json.loads(nested_json_string)
                            stmt_type_val = nested_json_object['stmt_type']
                            object_type_val = nested_json_object['object_type']
                            operation_type_val = nested_json_object['operation_type']
                            schema_name_val = nested_json_object['schema_name']
                            object_name_val = nested_json_object['object_name']

                            exist_query = f""" select count(*) from migration_script where sql_file_name = {row['SQL_File_Name']} and _stg_sql_file_md5 = {md5}"""
                            result = session.sql(exist_query).collect()

                            # if result is equals to zero, then only insert, else not.
                            if (len(result) == 0 ) :
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
                                session.sql(insert_sql, params=param_vals).collect()
                        except Exception as e:
                            # Handle exceptions and print an error message
                            print(f"An error occurred: {str(e)}")
                    classification_done = True

    # ---------------------
    # Display Stats
    st.markdown('<h4 style="color: #2596be;">Step-2 Object Classification Statistics</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">Classified Objects (Table/View etc) are stored in a snowflake table and grouped by to draw the bar chart.</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collapse", expanded=True): 
        if st.button('Show Stats'):
            col1, col2 = st.columns(2)
            with col1:  
                stats_df = helper_stats.draw_object_classification(session)
                st.bar_chart(stats_df,x="DB Object Types", y="Object Count", color=['#7fd9a8'])
            with col2:
                stats_df = helper_stats.draw_operation_classification(session)
                st.bar_chart(stats_df,x="Operation Types", y="Count", color=['#ffd65c'])
    
    # Generate Table DDLs 
    st.markdown('<h4 style="color: #2596be;">Step-3 Translate Table DDLs to Snowflake</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL Table DDL Scripts are passed to Snowflake cortex function for Snowflake compatible result is fetched</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collapse", expanded=True): 
        if st.button('Start SQL Translation For Table DDLs'):
            sql_body_df = helper_table_ddl.get_table_ddl_sql_body(session,25)

            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Table DDL</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)
            for index, row in sql_body_df.iterrows():
                st.divider()
                col1, col2 = st.columns(2)
                ms_sql_script = row['SQL_BODY']
                with col1: 
                    tab01, tab02 = st.tabs(['MS SQL Table DDL',row['SQL_FILE_NAME'] ])
                    with tab01:
                        st.code(ms_sql_script, language='sql' , line_numbers = True)
                    with tab02:
                        st.markdown(f"""
                            - File Name: {row['SQL_FILE_NAME']}
                            - File Load Time: {row['LOAD_TIME']}
                            - File MD5: {row['MD5']}
                            """)
                with col2:
                    sf_sql_script = helper_table_ddl.get_snowflake_sql(session,ms_sql_script)
                    nested_json_string = ''
                    tab01, tab02 = st.tabs(['Snowfake Table DDL', 'Snowflake Copy Command'])
                    with tab01:
                        for element in sf_sql_script:
                            json_body = element.as_dict()
                            nested_json_string = json_body['RESPONSE']
                            #nested_json_object = json.loads(nested_json_string)
                            st.code(nested_json_string, language='sql' , line_numbers = True)
                            update_query = """
                                update migration_script set 
                                    snowflake_sql_construct = ? , 
                                    snowflake_object_type = 'Table',
                                    updated_ts = current_timestamp()
                                    where
                                    sql_file_name = ? and 
                                    _stg_sql_file_md5 = ?

                            """
                            param_vals = [
                                nested_json_string,
                                row['SQL_FILE_NAME'],
                                row['MD5']
                            ]
                            session.sql(update_query, params=param_vals).collect()
                        with tab02:
                            copy_sript = helper_table_ddl.get_copy_command_sql(session,nested_json_string)
                            for element in copy_sript:
                                json_body = element.as_dict()
                                copy_str = json_body['RESPONSE']
                                st.code(copy_str, language='sql' , line_numbers = True)
    
    # Generate Function DDLs 
    st.markdown('<h4 style="color: #2596be;">Step-4 Translate Function (UDFs) DDLs to Snowflake</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL UDF DDL Scripts are passed to Snowflake cortex function for Snowflake compatible result is fetched</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collaps", expanded=True):     
        if st.button('Start SQL Translation For Function DDLs'):
            sql_body_df = helper_function_ddl.get_function_ddl_sql_body(session,5)
            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Function (UDF) DDL</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)
            for index, row in sql_body_df.iterrows():
                st.divider()
                col1, col2 = st.columns(2)
                ms_sql_script = row['SQL_BODY']
                with col1: 
                    tab01, tab02 = st.tabs(['MS SQL Function DDL',row['SQL_FILE_NAME'] ])
                    with tab01:
                        st.code(ms_sql_script, language='sql' , line_numbers = True)
                    with tab02:
                        st.write(row['SQL_FILE_NAME'])
                with col2:
                    st.markdown("##### Snowflake Function DDL")
                    sf_sql_script = helper_function_ddl.get_snowflake_sql(session,ms_sql_script)
                    for element in sf_sql_script:
                        json_body = element.as_dict()
                        nested_json_string = json_body['RESPONSE']
                        #nested_json_object = json.loads(nested_json_string)
                        st.code(nested_json_string, language='sql' , line_numbers = True)
                        update_query = """
                            update migration_script set 
                                snowflake_sql_construct = ? , 
                                snowflake_object_type = 'Table',
                                updated_ts = current_timestamp()
                                where
                                sql_file_name = ? and 
                                _stg_sql_file_md5 = ?

                        """
                        param_vals = [
                            nested_json_string,
                            row['SQL_FILE_NAME'],
                            row['MD5']
                        ]
                        session.sql(update_query, params=param_vals).collect()

    # Generate View DDLs 

    st.markdown('<h4 style="color: #2596be;">Step-5 Translate View DDLs to Snowflake</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL View DDL Scripts are passed to Snowflake cortex function for Snowflake compatible result is fetched</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collaps", expanded=True):     
        if st.button('Start SQL Translation For View DDLs'):
            sql_body_df = helper_view_ddl.get_view_ddl_sql_body(session,0)

            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Function (UDF) DDL</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)
            for index, row in sql_body_df.iterrows():
                st.divider()
                col1, col2 = st.columns(2)
                ms_sql_script = row['SQL_BODY']
                with col1: 
                    tab01, tab02 = st.tabs(['MS SQL View DDL',row['SQL_FILE_NAME'] ])
                    with tab01:
                        st.code(ms_sql_script, language='sql' , line_numbers = True)
                    with tab02:
                        st.write(row['SQL_FILE_NAME'])
                with col2:
                    tab01, tab02, tab03 = st.tabs(['View DDL', 'Summary', 'Dependencies'])
                    with tab01: 
                        sf_sql_script = helper_view_ddl.get_snowflake_sql(session,ms_sql_script)
                        for element in sf_sql_script:
                            json_body = element.as_dict()
                            nested_json_string = json_body['RESPONSE']
                            #nested_json_object = json.loads(nested_json_string)
                            st.code(nested_json_string, language='sql' , line_numbers = True)
                            update_query = """
                                update migration_script set 
                                    snowflake_sql_construct = ? , 
                                    snowflake_object_type = 'Table',
                                    updated_ts = current_timestamp()
                                    where
                                    sql_file_name = ? and 
                                    _stg_sql_file_md5 = ?

                            """
                            param_vals = [
                                nested_json_string,
                                row['SQL_FILE_NAME'],
                                row['MD5']
                            ]
                            session.sql(update_query, params=param_vals).collect()
                        with tab02: 
                            summary_txt_df = helper_view_ddl.get_view_summary(session,nested_json_string)
                            for element in summary_txt_df:
                                json_body = element.as_dict()
                                nested_json_string = json_body['RESPONSE']
                                st.write(nested_json_string)

                        with tab03: 
                            summary_txt_df = helper_view_ddl.get_view_dependency(session,nested_json_string)
                            for element in summary_txt_df:
                                json_body = element.as_dict()
                                nested_json_string = json_body['RESPONSE']
                                st.write(nested_json_string)

    # Generate Trigger DDLs

    st.markdown('<h4 style="color: #2596be;">Step-6 Translate Trigger DDLs to Snowflake</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL Trigger DDL Scripts are passed to Snowflake cortex function for Snowflake compatible result is fetched</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collaps", expanded=True):    
        if st.button('Start SQL Translation For Trigger DDLs'):
            sql_body_df = helper_trigger_ddl.get_trigger_ddl_sql_body(session,5)
            
            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Trigger Scripts</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)

            for index, row in sql_body_df.iterrows():
                st.divider()
                col1, col2 = st.columns(2)
                ms_sql_script = row['SQL_BODY']
                with col1: 
                    tab01, tab02 = st.tabs(['MS SQL Trigger DDL',row['SQL_FILE_NAME'] ])
                    with tab01:
                        st.code(ms_sql_script, language='sql' , line_numbers = True)
                    with tab02:
                        st.write(row['SQL_FILE_NAME'])
                with col2:
                    tab01, tab02 = st.tabs(['View DDL', 'Summary'])
                    with tab01: 
                        sf_sql_script = helper_trigger_ddl.get_snowflake_sql(session,ms_sql_script)
                        for element in sf_sql_script:
                            json_body = element.as_dict()
                            nested_json_string = json_body['RESPONSE']
                            #nested_json_object = json.loads(nested_json_string)
                            st.code(nested_json_string, language='sql' , line_numbers = True)
                            update_query = """
                                update migration_script set 
                                    snowflake_sql_construct = ? , 
                                    snowflake_object_type = 'Table',
                                    updated_ts = current_timestamp()
                                    where
                                    sql_file_name = ? and 
                                    _stg_sql_file_md5 = ?

                            """
                            param_vals = [
                                nested_json_string,
                                row['SQL_FILE_NAME'],
                                row['MD5']
                            ]
                            session.sql(update_query, params=param_vals).collect()
                        with tab02: 
                            summary_txt_df = helper_trigger_ddl.get_view_summary(session,nested_json_string)
                            for element in summary_txt_df:
                                json_body = element.as_dict()
                                nested_json_string = json_body['RESPONSE']
                                st.write(nested_json_string)

    # Generate Constraints DDL using alter
    st.markdown('<h4 style="color: #2596be;">Step-7: Translate Consraints SQL Scripts to Snowflake</h4>', unsafe_allow_html=True)
    st.markdown('<h6 style="color: #ffab40;">MS SQL Consraints Alter Scripts are passed to Snowflake cortex function for Snowflake compatible result is fetched</h6>', unsafe_allow_html=True)
    with st.expander("Expand/Collaps", expanded=True):       
        if st.button('Translate Consraints'):
            
            sql_body_df = helper_constraints_ddl.get_constraints_ddl_sql_body(session,10)

            col1, col2 = st.columns(2)
            with col1: 
                st.markdown('<h5 style="color: #d45b90ff;">MS SQL Server - Constraints Scripts</h5>', unsafe_allow_html=True)
            with col2:
                st.markdown('<h5 style="color: #d45b90ff;">Snowflake Arctic LLM Reponse</h5>', unsafe_allow_html=True)
            for index, row in sql_body_df.iterrows():
                st.divider()
                col1, col2 = st.columns(2)
                ms_sql_script = row['SQL_BODY']
                with col1: 
                    tab01, tab02 = st.tabs(['MS SQL Constraints Script',row['SQL_FILE_NAME'] ])
                    with tab01:
                        st.code(ms_sql_script, language='sql' , line_numbers = True)
                    with tab02:
                        st.write(row['SQL_FILE_NAME'])
                with col2:
                    st.markdown("##### Snowflake")
                    sf_sql_script = helper_constraints_ddl.get_snowflake_sql(session,ms_sql_script)
                    for element in sf_sql_script:
                        json_body = element.as_dict()
                        nested_json_string = json_body['RESPONSE']
                        #nested_json_object = json.loads(nested_json_string)
                        st.code(nested_json_string, language='sql' , line_numbers = True)
                        update_query = """
                            update migration_script set 
                                snowflake_sql_construct = ? , 
                                snowflake_object_type = 'None',
                                updated_ts = current_timestamp()
                                where
                                sql_file_name = ? and 
                                _stg_sql_file_md5 = ?

                        """
                        param_vals = [
                            nested_json_string,
                            row['SQL_FILE_NAME'],
                            row['MD5']
                        ]
                        session.sql(update_query, params=param_vals).collect()
