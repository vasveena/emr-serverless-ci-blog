FROM public.ecr.aws/emr-serverless/spark/emr-6.9.0:latest

USER root

# Install JDK 11
RUN amazon-linux-extras install java-openjdk11

# EMRS will run the image as hadoop
USER hadoop:hadoop
