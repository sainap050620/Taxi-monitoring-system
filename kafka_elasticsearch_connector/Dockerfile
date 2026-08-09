FROM docker.elastic.co/logstash/logstash:8.11.1

WORKDIR /kafka_elasticsearch_connector

COPY logstash.conf /usr/share/logstash/pipeline/logstash.conf
COPY template.json /usr/share/logstash/
COPY backup.sh ./backup.sh

RUN sed -i 's/\r$//' /usr/share/logstash/pipeline/logstash.conf
RUN sed -i 's/\r$//' /usr/share/logstash/template.json
RUN sed -i 's/\r$//' ./backup.sh

RUN chmod +x ./backup.sh

CMD ./backup.sh import && /usr/local/bin/docker-entrypoint