from snowflake.snowpark import Session

def get_sql_object_classification_json(session,ms_sql_body):
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
        {ms_sql_body}
        </context>
    """
    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    
    df_response = session.sql(cmd, params=['snowflake-arctic', prompt2]).collect()
    return df_response


