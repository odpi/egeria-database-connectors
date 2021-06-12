<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

![Egeria Logo](https://raw.githubusercontent.com/odpi/egeria/master/assets/img/ODPi_Egeria_Logo_color.png)

[![GitHub](https://img.shields.io/github/license/odpi/egeria)](LICENSE)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/3044/badge)](https://bestpractices.coreinfrastructure.org/projects/3044)
[![Maven Central](https://img.shields.io/maven-central/v/org.odpi.egeria/egeria)](https://mvnrepository.com/artifact/org.odpi.egeria)


# Egeria - Open Metadata and Governance
  
Egeria provides the Apache 2.0 licensed [open metadata and governance](open-metadata-publication/website/README.md)
type system, frameworks, APIs, event payloads and interchange protocols to enable tools,
engines and platforms to exchange metadata in order to get the best
value from data whilst ensuring it is properly governed.

# Postgres Connector

This project contains the Postgres Database Integration Daemon Connector for Egeria.
 

#Introduction


The Postgres Database Integration Daemon Connector is an example of an Egeria Integration Daemon connector, an integration framework which is described in detail [here.](https://egeria.odpi.org/open-metadata-implementation/admin-services/docs/concepts/integration-daemon.html#:~:text=An%20Integration%20Daemon%20is%20an,Access%20Point%20or%20Metadata%20Server.)
To support the documentation on Integration Daemons there is also a [Postman Collection](https://egeria.odpi.org/open-metadata-resources/open-metadata-tutorials/postman-tutorial/) which contains examples of the commands needed to enable the running of different types of integration daemon.
So this readme will only discuss the specific configuration needed to run an instance of the Postgres connector, as illustrated in the following diagram.
![alt text](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTrdVMpQA6q1dlb4x3iiDP0W5OG4d3UNZUCnw&usqp=CAU "possible integration daemon configuration")

#Overview
The Postgres Database Integration Daemon Connector works by periodically making a JDBC connection to a Postgres database instance running on Postgres server. It's not important which database accepts the connection as 
this connector retrieves the applicable metadata from Postgres provided system tables as an implementation of the SQL [information schema.](https://www.Postgresql.org/docs/9.1/information-schema.html)
so it is important that the user has the correct read permissions for this schema.
To se the correct privlidges for this sample the following command was used.

``
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO  postgresid;
``



The user ID that makes the JDBC connection to the Postgres Server must have the correct read privileges to access the metatdata for all the system entities.

This Integration Daemon Postgres connector supports the following types of database entity and will store the relevant metatdata for each corresponding egeria

* Databases
* Schemas
* Tables
* Views
* Columns
* Primary Keys
* Foreign Keys 

# Starting the Egeria Platform
Below is an example of the java commandline used to launch the Egeria platform that will host the Integration Daemon that is in turn hosting the 
Postgres connector instance we are about to configure.
The point of interest when launching a platform that will host connectors is the `-Dloader.path=/home/wbittles/egeria-database-connectors/Postgres-connector/libs` command line option.
The option specifies a file system directory that must contain both the Postgres JDBC driver and the Postgres-connector jar file.
````
java -Dloader.path=/home/wbittles/egeria-database-connectors/Postgres-connector/libs -Dserver.port=9443 -jar /home/wbittles/egeria/open-metadata-distribution/open-metadata-assemblies/target/egeria-2.10-SNAPSHOT-distribution/egeria-omag-2.10-SNAPSHOT/server/server-chassis-spring-2.10-SNAPSHOT.jar`
````

# Postgres Connection Details
After creating the Egeria Integration Daemon server that will host the Postgres connector, the next step is to configure the connector. 
As typical with Egeria the we configure the Postgres connector by posting a JSON configuration document to a REST API endpoint. 

In this readme we'll break down a cURL request, which is based on the following command from the Postman collection.
`Configure integration daemon server with an integration service`

Below we can see the typical REST endpoint and the associated header where 
`platformName` is the name of the server hosting the egeria platform which hosts the Integration Daemon, and
`integrationDaemonServer` is the name of the server that is to host the Postgres connector.
````shell
 curl --location --request POST 'https://platformName:9443/open-metadata/admin-services/users/garygeeke/servers/integrationDaemonServerName/integration-services/database-integrator' \
--header 'Content-Type: application/json' \
````

In the above section the
omagserverPlatformRootURL is the name of the OMAG platform that is hosting the OMAG Server that hosts the OMAS that the integration daemon is partnered with.
omagserverName is the name of OMAG server that is hosting the OMAS that this integration daemon is partnered with. These fields contain the information necessary
to connect to the Egeria Server which is hosting the Data Manager OMAS.

```shell

--data-raw '{
    "class": "IntegrationServiceRequestBody",
    "omagserverPlatformRootURL": "https://platformName:9443",
    "omagserverName" : "omagServer",
    
  ````
In the next section we see how to configure the name of this particular connector instance and the userID that will be used to connect to the 
egeria OMAS server.

```shell
    "integrationConnectorConfigs" : [ 
        {
             "class": "IntegrationConnectorConfig",
             "connectorName" : "connectorInstanceName",             
             "connectorUserId" : "egeriaUserID",
  ````   
In the next section we declare a connection object that will provide the details necessary to make a connection to the Postgres database.
We also provide the fully qualified Java classname for the Postgres connectors provider.
The `recognizedConfigurationProperties` and the `configurationProperties`sections provides the names of any parameters we wish to pass to the Postgres JDBC client.
In this example we just pass the jdbc connection url and set ssl to false. The full list of possible parameters are detailed in the Postgres documentation 
[here.](https://jdbc.Postgresql.org/documentation/head/connect.html)
```shell
  "connection" : 
             { 
                 "class" : "Connection",
                 "userId" : "postgresuserid",
                 "clearPassword":"postgrespassword",
                 "connectorType" : 
                 {
                     "class" : "ConnectorType",
                     "connectorProviderClassName" : "org.odpi.openmetadata.adapters.connectors.integration.Postgres.PostgresDatabaseProvider"
                     
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
`metadataSourceQualifiedName` specifies the Egeria qualified name of the entity that represents the Software Server entity represents the Postgres server.
If you have already created and entity to represent the Postgres server simply enter the qualified name here. If you haven't already created the server
entity, one will be automatically created by the integration daemon.
The 'refreshTimeInterval' specifies the time lapse between refresh cycles in minutes.
The 'permittedSynchronization' specifies the Integration Daemons thread synchronization model.

```shell

             "metadataSourceQualifiedName" : "PostgresHostServer",
             "refreshTimeInterval" : "3456", 
             "usesBlockingCalls" : "false",
             "permittedSynchronization" : "FROM_THIRD_PARTY"
        } ] 
}
````

[Egeria's Connector Catalog](https://egeria-project.org/open-metadata-publication/website/connector-catalog/)

License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.

