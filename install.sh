#!/usr/bin/env bash
docker build -t elk .
docker run -d --name elk -p 5601:5601 -p 9200:9200 -p 5044:5044 -e \
ES_HEAP_SIZE="8g" -e LS_HEAP_SIZE="1g" -v ~/data/logstash/:/logstash elk

docker exec -d elk service filebeat restart
#docker exec -it elk /bin/bash