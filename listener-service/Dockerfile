FROM ubuntu

WORKDIR /app

COPY entities.txt ./

RUN apt-get update && \
    apt-get install -y python3-pip curl &&  \
    pip3 install confluent-kafka &&  \
    pip3 install clickhouse-driver &&  \
    pip3 install retry &&  \
    pip3 install pydantic &&  \
    pip3 install requests

CMD ["sleep", "99999"]