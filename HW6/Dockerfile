FROM python:3.10-bullseye

WORKDIR /HW6

COPY requirements.txt .

RUN apt-get update && apt-get install -y default-jre && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "amazon_reviews.py"]