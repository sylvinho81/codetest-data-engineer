import logging

from typing import Any, Optional, Union

from clickhouse_driver.errors import NetworkError, SocketTimeoutError
from clickhouse_driver import Client
from retry import retry


logger = logging.getLogger(__name__)


class WrapperClickhouseClient:
    def __init__(self) -> None:
        self.client = Client(
            host='clickhouse',
            port=9000,
            user='default',
            database='analytics_saga',
            client_name="analytics_saga_client",
        )

    @retry(exceptions=(NetworkError, SocketTimeoutError), tries=5, delay=1.0, backoff=2.0)
    def execute(
        self,
        query: str,
        query_id: Optional[str] = None,
        params: Optional[Union[list[dict[str, Any]], dict[str, Any]]] = None,
    ) -> Any:
        return self.client.execute(query=query, params=params, query_id=query_id)

    def close(self) -> None:
        self.client.disconnect()
