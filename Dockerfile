FROM sebp/elk

#WORKDIR ${LOGSTASH_HOME}
#RUN gosu logstash bin/logstash-plugin install logstash-codec-gzip_lines

#ENV ES_HEAP_SIZE 8g
#ENV LS_HEAP_SIZE 1g

ADD 02-beats-input.conf /etc/logstash/conf.d/02-beats-input.conf
#ADD file-input.conf /etc/logstash/conf.d/file-input.conf
ADD filter.conf /etc/logstash/conf.d/filter.conf
ADD 30-output.conf /etc/logstash/conf.d/30-output.conf

RUN apt-get update && apt-get install -y wget htop

RUN wget https://artifacts.elastic.co/downloads/beats/filebeat/filebeat-6.3.1-amd64.deb
RUN dpkg -i filebeat-6.3.1-amd64.deb
RUN rm filebeat-6.3.1-amd64.deb

RUN mkdir -pv /etc/filebeat
ADD filebeat.yml /etc/filebeat/filebeat.yml
RUN chmod go-w /etc/filebeat/filebeat.yml
