FROM apache/airflow:2.8.2
LABEL authors="cedri"
WORKDIR .
COPY . .
# Install necessary packages
USER root
RUN apt-get update && \
    apt-get install -y wget gnupg2 software-properties-common python3-pip procps && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://packages.adoptium.net/artifactory/deb && \
    apt-get update && apt-get install -y temurin-17-jdk && \
    rm -rf /var/lib/apt/lists/*


#readlink -f $(which java)
ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install -r requirements.txt
