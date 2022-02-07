# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the Egeria project

# Thes are optional tags used to add additional metadata into the docker image
# These may be supplied by the pipeline in future - until then they will default

ARG egeriaversion=3.5
ARG baseimage=docker.io/odpi/egeria

# DEFER setting this for now, using the ${version}:
# ARG EGERIA_IMAGE_DEFAULT_TAG=latest

# This Dockerfile should be run from the parent directory of the egeria-connector-sas-viya directory
# ie
# docker -f ./Dockerfile 

FROM ${baseimage}:${egeriaversion}

ARG connectorversion=3.6-SNAPSHOT
ARG postgresurl=https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

#ENV connectorversion ${connectorversion}
#ENV egeriaversion ${egeriaversion}
#ENV postgresurl ${postgresurl}


# Labels from https://github.com/opencontainers/image-spec/blob/master/annotations.md#pre-defined-annotation-keys (with additions prefixed    ext)
# We should inherit all the base labels from the egeria image and only overwrite what is necessary.
LABEL org.opencontainers.image.description = "Egeria with Postgres connector" \
      org.opencontainers.image.documentation = "https://github.com/odpi/egeria-database-connectors"

# copy the connector
COPY egeria-connector-postgres/build/libs/egeria-connector-postgres-${connectorversion}.jar /deployments/server/lib
# get the postgres driver hardcoded version)
ADD ${postgresurl} /deployments/server/lib
# correct permissions from file download - we can only do this as root, and ADD doesn't have an option for chmod (only chown)
# so to maintain consistency (owned by root, globally readable) we fix here
USER root
RUN chmod -R guo+r /deployments/server/lib
# And return to the original user for security
USER jboss
