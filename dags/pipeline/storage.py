import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def save_df_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    postgres_conn_id: str = 'my_postgres',
    schema: str = None,
    if_exists: str = 'replace',
    index: bool = False,
    chunksize: int = 1000,
    **to_sql_kwargs
):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=index,
        chunksize=chunksize,
        method='multi',
        **to_sql_kwargs
    )
    
    engine.dispose()


def load_df_from_postgres(
    query: str,
    postgres_conn_id: str = 'my_postgres',
    index_col: str = None
) -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    engine = hook.get_sqlalchemy_engine()
    
    df = pd.read_sql(query, engine, index_col=index_col)
    
    engine.dispose()
    
    return df

def load_table_from_postgres(
    table_name: str,
    postgres_conn_id: str = 'my_postgres',
    schema: str = 'public',
    limit: int = None,
    columns: list = None
) -> pd.DataFrame:
    """Загружает целую таблицу"""
    query = f"SELECT {','.join(columns)} FROM {schema}.{table_name}" if columns else f"SELECT * FROM {schema}.{table_name}"
    return load_df_from_postgres(query, postgres_conn_id, schema, limit)