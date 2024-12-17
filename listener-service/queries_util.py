from typing import Any
from schemas import Entity, Saga
from functools import lru_cache
from clickhouse_connection import WrapperClickhouseClient


@lru_cache()
def load_entities(clickhouse_client: WrapperClickhouseClient) -> list[Entity]:
    query = f"SELECT entity_id, name FROM dim_entity"
    entities_raw = clickhouse_client.execute(query)
    entities = [Entity(entity_id=r[0], name=r[1]) for r in entities_raw]
    return entities


@lru_cache()
def load_sagas(clickhouse_client: WrapperClickhouseClient) -> list[Saga]:
    query = f"SELECT saga_id, name FROM dim_saga"
    sagas_raw = clickhouse_client.execute(query)
    sagas = [Saga(saga_id=r[0], name=r[1]) for r in sagas_raw]
    return sagas


def insert_entities(clickhouse_client: WrapperClickhouseClient, fact_readers_json: list[dict[str, Any]]) -> None:
    query = """
        INSERT INTO fact_readers
            (id, saga_id, entity_id, entity_counter, readers, date_time)
        VALUES
    """

    clickhouse_client.execute(query=query, params=fact_readers_json)