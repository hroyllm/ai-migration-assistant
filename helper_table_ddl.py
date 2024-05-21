import pandas as pd

def get_snowflake_sql(session,sql_body):
    prompt = f"""
        You are a SQL translator expert, can translated Microsoft SQL Server table DDL to Snowflake DDLs. 
        The SQL Server compatible sqls given within <context> and </context> tags. 
        If schema is dbo, then exlude the schema name in the output.
        Have the table/column names in uppercase without double quotes.
        NVARCHAR/VARCHAR/STRING, all such datatype should be converted into text
        xml/json data type should be converted into variant.
        User CREATE OR REPLACE for table creation.
        The response should be provided in plain text format and it should be just DDL statement, nothing else.
        <context>
        {sql_body}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response

def get_copy_command_sql(session,sql_body):
    prompt = f"""
        You are an snowflake  SQL  expert, can write any kind of SQL supported by Snowflake. 
        Here you have to take a table structure and generate a copy sql statement.
        Assume the DATA_STG as internal stage location where files are available in csv format.
        the name of the csv matches with name of the table and your job is construct SQL statement for copy statement
        The table DDL sqls is given within <context> and </context> tags. 
        copy command should also add continue on error parameter.
       
        <context>
        {sql_body}
        </context>

        your final response should be provided in plain text format and it should be just copy sql statement, nothing else.
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt]).collect()
    return df_response

#Execute query and return panda's df
def get_table_ddl_sql_body(session, limit=0):

    limit_clause = ''
    if limit > 0:
        limit_clause = 'limit '+str(limit)

    #Post Execution, Draw The Plot
    sql_script_query = f"""
            select 
                sql_file_name,
                _stg_sql_file_md5,
                _stg_sql_file_load_ts,
                sql_body
            from 
                migration_script
            where 
                stmt_type = 'DDL'
                and object_type = 'table'
                and operation_type = 'create'
            {limit_clause}
    """
    sql_db_df = session.sql(sql_script_query).collect()
    pd_sql_df = pd.DataFrame(sql_db_df,columns=['SQL_FILE_NAME','MD5','LOAD_TIME','SQL_BODY'])
    return pd_sql_df