import pandas as pd

#Execute query and return panda's df
def get_trigger_ddl_sql_body(session, limit=0):

    limit_clause = ''
    if limit > 0:
        limit_clause = 'limit '+str(limit)

    #Post Execution, Draw The Plot
    sql_script_query = f"""
            select 
                sql_file_name,
                _stg_sql_file_md5,
                sql_body
            from 
                migration_script
            where 
                stmt_type = 'DDL'
                and object_type = 'trigger'
                and operation_type = 'create'
            {limit_clause}
    """

    sql_db_df = session.sql(sql_script_query).collect()
    pd_sql_df = pd.DataFrame(sql_db_df,columns=['SQL_FILE_NAME','MD5','SQL_BODY'])
    return pd_sql_df

def get_snowflake_sql(session,sql_body):
    prompt = f"""
        You are a SQL translator expert, can translated Microsoft SQL Server triggers to Snowflake tasks.
        The SQL Server compatible SQLs given within <context> and </context> tags. 
        
        Since snowflake does not support trigger, same has to be achived using snowflake task objects.
        The trigger logic should be part of a stored procedure that has to be created before creating task. 
        
        Make sure the final SQL snowflake output should be well formatted and apply standard SQL linting.
        All the select column in view must be a new line in the output.
        if there is a schema called dbo, don't include the schema name.

        The response should be provided just SQL in well formatted manner and no additional detail is needed.
        
        <context>
        {sql_body}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response
    

def get_view_summary(session,view_sql):
    prompt = f"""
        You are SQL expert and can summaries any kind of standard (from medium to complex) SQL in simple terms.
        The SQL script is provided within <context> and </context> tags. 
        
        You job is to provide summary of the SQL, what it is trying to do and what output it will generate when executed.

        Provide your summarization in plain English in less than 100 words that is easy to understand.
        Finaly add the query complexity with single world like Simple, Medium, Complex or Very Complex.

        Example Reponse
        Summary: 
        Expected Output:
        Query Complexity:

        Do not mention the CONTEXT used in your answer.
        Be concise and do not hallucinate. 

        <context>
        {view_sql}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response