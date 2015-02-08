############################################################
# Dockerfile to run Neverdown inside a container
# Based on Ubuntu Image
############################################################
FROM ubuntu
MAINTAINER Thomas Sileo
EXPOSE 8050
ADD ./blobstash /opt/blobstash
ENTRYPOINT /opt/blobstash
