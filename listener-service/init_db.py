import logging

from os import path
from clickhouse_connection import WrapperClickhouseClient
from uuid import uuid4


SQL_FOLDER = "sql"

logger = logging.getLogger(__name__)

schema_files = [
    "create_dim_entities_table.sql",
    "create_dim_sagas_table.sql",
    "create_fact_readers_table.sql",
]

drop_tables_files = [
    "drop_table_dim_entities_table.sql",
    "drop_table_dim_sagas_table.sql",
    "drop_table_fact_readers_table.sql",
]


def load_schema_statement(file_path: str) -> str:
    """Read query from the given sql file path"""
    abs_path = path.curdir
    full_path = path.join(abs_path, SQL_FOLDER, file_path)
    logging.info(f"Query file to read from: {file_path}")

    with open(full_path) as f:
        query = f.read()

    return query


def insert_entities(clickhouse_client: WrapperClickhouseClient) -> None:
    entities_set = set()

    with open("entities.txt") as file:
        for entity in file:
            entity_name = entity.rstrip('\n')
            entities_set.add(entity_name)

    # Now entities_dict contains unique entities based on the "name" field
    entities_json = [{"entity_id": str(uuid4()), "name": entity_name} for entity_name in entities_set]
    query = """
        INSERT INTO dim_entity
            (entity_id, name)
        VALUES
    """

    clickhouse_client.execute(query=query, params=entities_json)


def insert_sagas(clickhouse_client: WrapperClickhouseClient) -> None:
    sagas_json = [{"saga_id": str(uuid4()), "name": "got"},
                  {"saga_id": str(uuid4()), "name": "lotr"},
                  {"saga_id": str(uuid4()), "name": "hp"}]

    query = """
        INSERT INTO dim_saga
            (saga_id, name)
        VALUES
    """

    clickhouse_client.execute(query=query, params=sagas_json)


def init_db(clickhouse_client: WrapperClickhouseClient) -> None:
    for drop_table_file in drop_tables_files:
        clickhouse_client.execute(load_schema_statement(drop_table_file), query_id=drop_table_file)
    for schema_file in schema_files:
        clickhouse_client.execute(load_schema_statement(schema_file), query_id=schema_file)
    insert_sagas(clickhouse_client=clickhouse_client)
    insert_entities(clickhouse_client=clickhouse_client)



