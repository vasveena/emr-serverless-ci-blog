FROM public.ecr.aws/emr-serverless/spark/emr-6.9.0:latest

USER root

# Install Python packages
RUN pip3 install boto3 pandas
RUN pip3 install numpy==1.20.2
RUN pip3 install sagemaker sagemaker_pyspark
RUN pip3 install matplotlib==3.2.0
RUN pip3 install xgboost

# EMRS will run the image as hadoop
USER hadoop:hadoop
