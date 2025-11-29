FROM apache/spark:3.5.2
LABEL authors="cedri"

USER root

WORKDIR /opt/bitnami/spark/jobs/
COPY . .

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

RUN chmod +x /opt/bitnami/spark/jobs/entrypoint.bash

ENTRYPOINT ["bash", "/opt/bitnami/spark/jobs/entrypoint.bash"]

CMD []
