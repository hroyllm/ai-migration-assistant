import pandas as pd


def get_snowflake_sql(session,sql_body):
    prompt = f"""
        [INST]
        You are a SQL translator expert, can translated Microsoft SQL Server statement to Snowflake Cloud SQL.
        The SQL Server compatible SQLs given within <context> and </context> tags. 
        
        The SQL scripts are adding referential integrity to table using alter statement.
        Your job is to understand them and translate them to equivalent Snowflake Cloud compatible SQL.

        While translating, there are important note that needs to be take care
        1. If there is a schema called dbo, don't include the schema name anywhere.
        2. Use upper case for all table and column names in the SQL statement
        3. Don't use double quotes for table name and column names.

        The response should be provided just SQL in well formatted manner and no additional detail is needed.
        
        <context>
        {sql_body}
        </context>
        [/INST]
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response

#Execute query and return panda's df
def get_constraints_ddl_sql_body(session, limit=3):

    limit_clause = ''
    if limit > 0:
        limit_clause = 'limit '+str(limit)

    sql_script_query = f"""
            select 
                sql_file_name,
                _stg_sql_file_md5,
                sql_body
            from 
                migration_script
            where 
                stmt_type = 'DDL'
                and object_type = 'table'
                and operation_type = 'alter'
            {limit_clause}
    """

    sql_db_df = session.sql(sql_script_query).collect()
    pd_sql_df = pd.DataFrame(sql_db_df,columns=['SQL_FILE_NAME','MD5','SQL_BODY'])
    return pd_sql_df