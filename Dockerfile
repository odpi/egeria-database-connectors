# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the Egeria project

# Thes are optional tags used to add additional metadata into the docker image
# These may be supplied by the pipeline in future - until then they will default

ARG version=latest
ARG postgresurl=https://jdbc.postgresql.org/download/postgresql-42.2.21.jar
ARG EGERIA_BASE_IMAGE=odpi/egeria
# DEFER setting this for now, using the ${version}:
# ARG EGERIA_IMAGE_DEFAULT_TAG=latest

# This Dockerfile should be run from the parent directory of the egeria-connector-sas-viya directory
# ie
# docker -f ./Dockerfile 

FROM ${EGERIA_BASE_IMAGE}:${version}


ENV version ${version}
ENV postgresurl ${postgresurl}


# Labels from https://github.com/opencontainers/image-spec/blob/master/annotations.md#pre-defined-annotation-keys (with additions prefixed    ext)
# We should inherit all the base labels from the egeria image and only overwrite what is necessary.
LABEL org.opencontainers.image.description = "Egeria with Postgres connector" \
      org.opencontainers.image.documentation = "https://github.com/odpi/egeria-database-connectors"

WORKDIR .
# copy the connector
COPY egeria-connector-postgres/build/libs-${version}*.jar /deployments/server/lib
# get the postgres driver hardcoded version)
ADD ${postgresurl} /deployments/server/lib
