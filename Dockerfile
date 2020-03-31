FROM yandex/clickhouse-server
COPY config.xml /etc/clickhouse-server/config.xml
RUN chmod 777 /etc/clickhouse-server/config.xml