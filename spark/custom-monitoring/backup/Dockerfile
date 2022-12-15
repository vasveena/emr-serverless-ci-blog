FROM public.ecr.aws/emr-serverless/spark3/emr-6.9.0:latest

USER root

RUN yum install -y wget

RUN wget https://github.com/prometheus/prometheus/releases/download/v2.26.0/prometheus-2.26.0.linux-amd64.tar.gz
RUN tar -xvf prometheus-2.26.0.linux-amd64.tar.gz
RUN cp prometheus-2.26.0.linux-amd64/prometheus /usr/local/bin/

RUN cp /etc/spark/conf.dist/metrics.properties.template /etc/spark/conf/metrics.properties

RUN echo -e '\
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet\n\
*.sink.prometheusServlet.path=/metrics/prometheus\n\
master.sink.prometheusServlet.path=/metrics/master/prometheus\n\
applications.sink.prometheusServlet.path=/metrics/applications/prometheus\n\
' >> /etc/spark/conf/metrics.properties

#RUN cat /etc/spark/conf/metrics.properties

RUN echo -e '\
global:\n\
  scrape_interval: 20s\n\
\n\
scrape_configs:\n\
  - job_name: 'spark-driver'\n\
    metrics_path: '/metrics/prometheus'\n\
    static_configs:\n\
      - targets: ['localhost:4040']\n\
        labels:\n\
            instance_type: 'driver'\n\
            spark_cluster: 'emrserverless'\n\
  - job_name: 'spark-executor'\n\
    metrics_path: '/metrics/executors/prometheus'\n\
    static_configs:\n\
      - targets: ['localhost:4040']\n\
        labels:\n\
            instance_type: 'driver'\n\
            spark_cluster: 'emrserverless'\n\
remote_write:\n\
  -\n\
    url: https://aps-workspaces.us-east-1.amazonaws.com/workspaces/ws-634ca82f-a423-4c52-8f6f-3d52fe0a8b2b/api/v1/remote_write\n\
    queue_config:\n\
        max_samples_per_send: 1000\n\
        max_shards: 200\n\
        capacity: 2500\n\
    sigv4:\n\
        region: us-east-1\n\
' >> /home/hadoop/prometheus.yaml

#RUN cat prometheus.yaml 
#RUN find / -name prometheus.yaml 

RUN echo -e '#!/bin/bash\n\
/usr/local/bin/prometheus --config.file=/home/hadoop/prometheus.yaml &\n\
while true\n\
do\n\
    curl -s http://localhost:4040/metrics/executors/prometheus | aws s3 cp - s3://vasveena-test-demo/prom/file$d.txt\n\
    d=$((d+1))\n\
    sleep 20\n\
done\n\
' >> /home/hadoop/script.sh

RUN echo -e '#!/bin/bash\n\
sh script.sh &\n\
'  > runner.sh

RUN chmod +x /home/hadoop/script.sh

RUN chmod +x /home/hadoop/runner.sh

#ENTRYPOINT [ "/home/hadoop/runner.sh" ]

CMD nohup /home/hadoop/runner.sh & sleep infinity

USER hadoop:hadoop
