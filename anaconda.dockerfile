FROM continuumio/anaconda3
MAINTAINER Andre Pruner
RUN apt-get update && apt-get install nano  
RUN /opt/conda/bin/conda update -n base -c defaults conda && \
    /opt/conda/bin/conda install python=3.9.7 && \
    /opt/conda/bin/conda install pandas=1.3.4 && \
    /opt/conda/bin/conda install urllib3=1.26 && \
    /opt/conda/bin/conda install openjdk=11.0.13 &&  \
    /opt/conda/bin/conda install pyspark=3.1.2 &&  \
    /opt/conda/bin/conda install boto3
EXPOSE 8080




