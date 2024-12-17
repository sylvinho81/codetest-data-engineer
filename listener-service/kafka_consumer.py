import logging
import json

from confluent_kafka import Consumer
from clickhouse_connection import WrapperClickhouseClient
from init_db import init_db
from queries_util import load_sagas, load_entities, insert_entities
from classification_api_service import ClassificationApiService
from schemas import ClassificationResult, Saga, Entity

logger = logging.getLogger(__name__)

ch_client = WrapperClickhouseClient()
classification_api = ClassificationApiService()

init_db(clickhouse_client=ch_client)
sagas: list[Saga] = load_sagas(clickhouse_client=ch_client)
entities: list[Entity] = load_entities(clickhouse_client=ch_client)


def count_occurrences(text: str, entities: list[Entity]) -> dict:
    # Create a dictionary with counts for entity_ids in entities list
    # TODO: with this approach it could happen that you have 'Sam Gamgee' and 'Sam', so this will detect
    # 'Sam' when maybe it shouldn't because the complete name is 'Sam Gamgee'.
    # We could try to use and entity recognition algorithm to extract the names by using LLM or a ML model.
    occurrences = {entity.entity_id: text.count(entity.name) for entity in entities}

    return occurrences


c = Consumer({
    'bootstrap.servers': 'kafka',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['events'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    event_decoded = msg.value().decode('utf-8')
    print('Received message: {}'.format(event_decoded))

    json_object = json.loads(event_decoded)
    text = json_object["text"]

    # if the text is empty it doesn't make sense to call the api because we will not be getting results
    if text:
        time = json_object["time"]
        readers = json_object["readers"]
        text_id = json_object["id"]

        classification_result: ClassificationResult = classification_api.api_call(text=text)
        saga_label = classification_result.label
        saga_id = next((saga.saga_id for saga in sagas if saga.name == saga_label), None)
        fact_readers_json = []
        if saga_id is not None:
            entities_counter_dict = count_occurrences(text=text, entities=entities)
            for entity_id, counter in entities_counter_dict.items():
                if counter > 0:
                    fact_readers_json.append({"id": str(text_id), "saga_id": str(saga_id),
                                              "entity_id": str(entity_id), "entity_counter": int(counter),
                                              "readers": int(readers), "date_time": int(time)})

            if fact_readers_json:
                insert_entities(clickhouse_client=ch_client, fact_readers_json=fact_readers_json)

c.close()
