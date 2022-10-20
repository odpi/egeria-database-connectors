<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

![Egeria Logo](https://raw.githubusercontent.com/odpi/egeria/main/assets/img/ODPi_Egeria_Logo_color.png)

[![GitHub](https://img.shields.io/github/license/odpi/egeria)](LICENSE)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3044/badge)](https://bestpractices.coreinfrastructure.org/projects/3044)
[![Maven Central](https://img.shields.io/maven-central/v/org.odpi.egeria/egeria-connector-postgres)](https://mvnrepository.com/artifact/org.odpi.egeria/egeria-connector-postgres)

# Egeria - Open Metadata and Governance
  
Egeria provides the Apache 2.0 licensed open metadata and governance 
type system, frameworks, APIs, event payloads and interchange protocols to enable tools,
engines and platforms to exchange metadata in order to get the best
value from data whilst ensuring it is properly governed.

[Go to the main Egeria site](https://egeria-project.org)

# Postgres Connector

This project currently contains the [Postgres](https://www.postgresql.org) Database [Integration Connector](https://egeria-project.org/concepts/integration-daemon/) for Egeria.

# Introduction

The Postgres Database Integration Connector is an example of an Egeria Integration connector, an integration framework which is described in detail [here.](https://egeria-project.org/concepts/integration-daemon/).

The Egeria [Postman Collection](https://egeria-project.org/education/tutorials/postman-tutorial/overview/?h=postman) contains examples of the commands needed to enable configuration and use of the integration daemon.

This readme will only cover the specific configuration needed to run an instance of the Postgres connector.

# Overview
The Postgres Database Integration Connector works by periodically making a JDBC connection to a Postgres database instance running on Postgres server. 

It's not important which database accepts the connection as 
this connector retrieves the applicable metadata from Postgres provided system tables as an implementation of the SQL [information schema.](https://www.postgresql.org/docs/14/information-schema.html), so it is important that the user has the correct read permissions for this schema.

The userid that makes the JDBC connection to the Postgres Server must have the correct read privileges to access the metadata for all the system entities.

To set the correct permissions for this sample the following command was used.

```
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO  postgresid;
```

This Integration Postgres connector supports the following types of database asset: 

* Databases
* Schemas
* Tables
* Views
* Columns
* Primary Keys
* Foreign Keys 

# Additional Container Images & Chart Information
If using the Egeria container image see [container docs](https://github.com/odpi/egeria/tree/main/open-metadata-resources/open-metadata-deployment/docker/egeria) 'Extending the image' which refers to how to add additional connectors. Alternatively use the [container image built here](https://quay.io/repository/odpi/egeria-database-connectors), which contains the version of Egeria the connector was built with, and the connector itself.

If using the egeria-base helm chart see [egeria-base docs](https://egeria-project.org/guides/operations/kubernetes/charts/base/#accessing-egeria) 'Extending the image' which refers to how to add additional connectors. 

# Running the connector

In this section we will discuss running Egeria natively. 

## Downloading required jars
First download the jar file containing the latest connector by running:
```
cd ~/pglib
mvn dependency:get  -DrepoUrl=https://repo1.maven.org/maven2/  -Dartifact=org.odpi.egeria:egeria-connector-postgres:LATEST:jar  -Dtransitive=false    -Ddest=egeria-connector-postgres.jar
```

Also download the latest version of the [postgres jdbc driver](https://jdbc.postgresql.org/download.html) and place in the same directory as the connector jar file. 

Note: If you build this project it can also be found in `./egeria-connector-postgres/build/libs`.

## Setting the loader.path & running the server chassis
When launching a platform that will host connectors it is essential to set the Spring classpath, using either the `LOADER_PATH=~/pglib` environment variable, or the a command line option such as `-Dloader.path=~/pglib` when running the  server chassis.

The directory specified must contain both the Postgres JDBC driver and the Postgres-connector jar file.
```
java -Dloader.path=/home/testuser/pglib -Dserver.port=9443 -jar /home/testuser/egeria/open-metadata-distribution/open-metadata-assemblies/target/egeria-3.5-distribution/egeria-omag-3.5/server/server-chassis-spring-3.5.jar`
```


# Postgres Connection Details
After creating the Egeria Integration Daemon server that will host the Postgres connector, the next step is to configure the connector. 
As typical with Egeria we configure the Postgres connector by posting a JSON configuration document to a REST API endpoint. 

In this readme we'll break down a cURL request, which is based on the following command from the Postman collection.
`Configure integration daemon server with an integration service`

Below we can see the typical REST endpoint and the associated header where 
`platformName` is the URL of the Egeria platform which hosts the Integration Daemon, and
`integrationDaemonServer` is the name of the server that is to host the Postgres connector.
```shell
 curl --location --request POST 'https://platformName:9443/open-metadata/admin-services/users/garygeeke/servers/integrationDaemonServerName/integration-services/database-integrator' \
--header 'Content-Type: application/json' \
```

In the above section the
omagserverPlatformRootURL is the URL of the OMAG Server that hosts the OMAS that the integration daemon is partnered with.
omagserverName is the name of OMAG server that is hosting the OMAS that this integration daemon is partnered with. These fields contain the information necessary
to connect to the Egeria Server which is hosting the Data Manager OMAS.

```shell

--data-raw '{
    "class": "IntegrationServiceRequestBody",
    "omagserverPlatformRootURL": "https://platformName:9443",
    "omagserverName" : "omagServer",
    
```
In the next section we see how to configure the name of this particular connector instance and the userID that will be used to connect to the 
egeria OMAS server.

```shell
    "integrationConnectorConfigs" : [ 
        {
             "class": "IntegrationConnectorConfig",
             "connectorName" : "connectorInstanceName",             
             "connectorUserId" : "egeriaUserID",
```   
In the next section we declare a connection object that will provide the details necessary to make a connection to the Postgres database.
We also provide the fully qualified Java classname for the Postgres connectors provider.
The `recognizedConfigurationProperties` and the `configurationProperties` sections provides the names of any parameters we wish to pass to the Postgres JDBC client.
In this example we just pass the jdbc connection url and set ssl to false. The full list of possible parameters are detailed in the Postgres documentation 
[here.](https://jdbc.postgresql.org/documentation/head/connect.html)
```shell
  "connection" : 
             { 
                 "class" : "Connection",
                 "userId" : "postgresuserid",
                 "clearPassword":"postgrespassword",
                 "connectorType" : 
                 {
                     "class" : "ConnectorType",
                     "connectorProviderClassName" : "org.odpi.openmetadata.adapters.connectors.integration.postgres.PostgresDatabaseProvider"
                     
                 },
                  "recognizedConfigurationProperties": [
                            "url",
                            "ssl"
                        ],
                 "configurationProperties":
                {
                    "url": "jdbc:postgresql://localhost:5432/postgres",
                    "ssl" : "false"
                }
             },
```
In the final section of the request body is where we set the final configuration properties.
`metadataSourceQualifiedName` specifies the Egeria qualified name of the Software Server entity that represents the Postgres server.
If you have already created an entity to represent the Postgres server simply enter the qualified name here. If you haven't already created the server
entity, one will be automatically created by the integration daemon.
The 'refreshTimeInterval' specifies the time between refresh cycles in minutes.
The 'permittedSynchronization' specifies the Integration Daemons thread synchronization model.

```shell

             "metadataSourceQualifiedName" : "postgreshostserver",
             "refreshTimeInterval" : "3456", 
             "usesBlockingCalls" : "false",
             "permittedSynchronization" : "FROM_THIRD_PARTY"
        } ] 
}
```
Further information on configuring an integration connector can be found in  [Configuring the Integration Services](https://egeria-project.org/guides/admin/servers/configuring-the-integration-services/) .

[Egeria's Connector Catalog](https://egeria-project.org/connectors/)

License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.

