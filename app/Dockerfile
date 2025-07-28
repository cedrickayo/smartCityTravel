FROM python:3.11-slim
LABEL authors="cedri"

WORKDIR /app
COPY . /app

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

RUN pip install --no-cache-dir -r requirements.txt

#CMD ["sh"]

CMD ["python", "./ModelCity.py"]
#ENTRYPOINT ["top", "-b"]