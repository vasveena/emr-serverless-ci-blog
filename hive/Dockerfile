FROM public.ecr.aws/emr-serverless/hive/emr-6.9.0:latest

USER root

# Install US census data to the image 
ADD https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_state_500k.zip  /usr/local/share/
ADD https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip /usr/local/share/
RUN chmod 644 /usr/local/share/cb*.zip

# TODO - UDF

# Install Prometheus agent 

RUN wget https://github.com/prometheus/prometheus/releases/download/v2.26.0/prometheus-2.26.0.linux-amd64.tar.gz
RUN tar -xvf prometheus-2.26.0.linux-amd64.tar.gz
RUN sudo cp prometheus-2.26.0.linux-amd64/prometheus /usr/local/bin/

RUN echo -e '\
global:\n\
  scrape_interval: 15s\n\
  external_labels:\n\
    monitor: 'prometheus'\n\
\n\
scrape_configs:\n\
  - job_name: 'prometheus'\n\
    static_configs:\n\
      - targets: ['localhost:8000']\n\
remote_write:\n\
  -\n\
    url: https://aps-workspaces.us-east-1.amazonaws.com/workspaces/ws-df17c698-d870-4ae0-ad60-57351d62ae3f/api/v1/remote_write\n\
    queue_config:\n\
        max_samples_per_send: 1000\n\
        max_shards: 200\n\
        capacity: 2500\n\
    sigv4:\n\
        region: us-east-1\n\
' >> prometheus.yaml

RUN prometheus --config.file=prometheus.yaml

USER hadoop:hadoop
