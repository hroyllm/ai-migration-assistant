from snowflake.snowpark import Session
import pandas as pd


def draw_object_classification(session):

    #Post Execution, Draw The Plot
    sql_script_query = """
            select 
                INITCAP(object_type) ,
                count(1) 
            from migration_script
            where 
                stmt_type = 'DDL'
            group by 1
            order by 2 desc
    """

    object_counts_df = session.sql(sql_script_query).collect()
    pd_df = pd.DataFrame(object_counts_df,columns=['DB Object Types','Object Count'])
    return pd_df

def draw_operation_classification(session):

    #Post Execution, Draw The Plot
    sql_script_query = """
            select 
                stmt_type ,
                count(1) 
            from migration_script
            group by 1
            order by 2 desc
    """

    object_counts_df = session.sql(sql_script_query).collect()
    pd_df = pd.DataFrame(object_counts_df,columns=['Operation Types','Count'])
    return pd_df