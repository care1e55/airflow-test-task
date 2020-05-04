FROM puckel/docker-airflow
USER root
RUN pip install --user psycopg2-binary
ENV AIRFLOW_HOME=/usr/local/airflow
RUN mkdir install_client
WORKDIR "/install_client"
RUN echo "deb [trusted=yes] http://repo.yandex.ru/clickhouse/deb/stable/ main/" >> /etc/apt/sources.list
RUN cat /etc/apt/sources.list
RUN apt-get update && \
apt-get install -y --allow-unauthenticated clickhouse-client 
RUN groupadd --gid 999 docker \
    && usermod -aG docker airflow
RUN chmod -R 777 /usr/local/airflow
RUN pip install clickhouse-sqlalchemy
USER airflow