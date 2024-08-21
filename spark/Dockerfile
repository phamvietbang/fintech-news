FROM bitnami/spark:latest

# Copy SSL certificates and Spark configuration
RUN mkdir /opt/bitnami/spark/certs
RUN chmod -R 777 /opt/bitnami/spark/certs
RUN chmod -R 777 /opt/bitnami/spark/conf/./
RUN chmod -R 777 /opt/bitnami/spark/conf/
COPY keystore.jks /opt/bitnami/spark/certs/keystore.jks
COPY truststore.jks /opt/bitnami/spark/certs/truststore.jks
COPY spark-defaults.conf /opt/bitnami/spark/certs/spark-defaults.conf