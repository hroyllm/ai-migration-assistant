import pandas as pd

def get_snowflake_sql(session,sql_body):
    prompt = f"""
        You are a SQL expert, can translated SQL Server SQL script to Snowflake SQL. 
        The SQL Server compatible SQLs given within <context> and </context> tags. 

        If variable name has @ sign, remove it in the output. 
        All text/string data type for RETURN keyword should be mapped to text data type in snowflake.
        Use 'LANGUAGE SQL' in snowflake function/UDFs script tye

        Snowflake UDF does not support DECLARE block so watch out.
        Snowflake functions does not suppor conditional block like if/else, so watch out.
        The response should be provided in plain text format and it should be just DDL SQL statement, nothing else.
        <context>
        {sql_body}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response

#Execute query and return panda's df
def get_function_ddl_sql_body(session, limit=0):

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
                and object_type = 'function'
                and operation_type = 'create'
            {limit_clause}
    """

    sql_db_df = session.sql(sql_script_query).collect()
    pd_sql_df = pd.DataFrame(sql_db_df,columns=['SQL_FILE_NAME','MD5','SQL_BODY'])
    return pd_sql_df
