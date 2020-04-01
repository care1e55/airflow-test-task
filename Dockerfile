FROM yandex/clickhouse-server
COPY config.xml /etc/clickhouse-server/config.xml
ADD initdb.sql /docker-entrypoint-initdb.d/initdb.sql
RUN chmod 777 /etc/clickhouse-server/config.xml
RUN chmod 777 /docker-entrypoint-initdb.d/initdb.sql