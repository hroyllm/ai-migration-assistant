import pandas as pd
def get_snowflake_sql(session,sql_body):
    prompt = f"""
        You are a SQL translator expert, can translated Microsoft SQL Server view DDL to Snowflake DDLs.  
        The SQL Server compatible SQLs given within <context> and </context> tags. 
        While creating snowflake DDLs, use create or replace view and also use upper case for view name including all column names.
        Don't enclose the view name or column names in double quotes.
        
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

def get_view_dependency(session,view_sql):
    prompt = f"""
        You are SQL expert and can understand all kind of SQL queries, be it DDL or DML or views.
        The SQL script is provided within <sql-view-ddl> and </sql-view-ddl> tags. 
        
        You job is to provide understnad the SQL for the provided view, and identify how many tables or views this view is depended on.

        Provide the name of the dependend tables and/or views (part of join query in the view) in the list form and don't add any summary. 
        For example
        This depends on following views and tables
        1. Table-x
        2. View-y
        3. and so on..

        Do not mention the CONTEXT used in your answer.
        be concise and do not hallucinate. 

        <context>
        {view_sql}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response

#Execute query and return panda's df
def get_view_ddl_sql_body(session, limit=0):

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
                and object_type = 'view'
                and operation_type = 'create'
            {limit_clause}
    """

    sql_db_df = session.sql(sql_script_query).collect()
    pd_sql_df = pd.DataFrame(sql_db_df,columns=['SQL_FILE_NAME','MD5','SQL_BODY'])
    return pd_sql_df