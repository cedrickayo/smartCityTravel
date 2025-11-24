FROM apache/spark:3.5.2
LABEL authors="cedri"

USER root

WORKDIR /opt/bitnami/spark/jobs/
COPY . /opt/bitnami/spark/jobs/

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

RUN chmod +x /opt/bitnami/spark/jobs/entrypoint.bash

ENTRYPOINT ["sh", "/opt/bitnami/spark/jobs/entrypoint.sh"]

CMD []
