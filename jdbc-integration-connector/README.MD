<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# JDBC Integration Connector

Catalogs a database via JDBC, extracting as much information as it can, as opposed to the connection structure that will be constructed if all information is available

### Configuration

For more details regarding bellow request, visit [Egeria documentation site](https://egeria-project.org/guides/admin/servers/configuring-an-integration-daemon/#configure-the-integration-services)

In addition, the integration connector uses a VirtualConnection in order to make use of the [JDBC resource connector](../jdbc-resource-connector/README.MD), which keeps the connection details to target database 
```
POST {{baseURL}}/open-metadata/admin-services/users/{{user}}/servers/{{server}}/integration-services/{{integrationServiceURLMarker}}
```
Body
```
{
    "class": "IntegrationServiceRequestBody",
    "omagserverPlatformRootURL": "<access-service-omag-url>",
    "omagserverName": "<omas-server-name>",
    "integrationConnectorConfigs":[ 
        {
            "class": "IntegrationConnectorConfig",
            "connectorName": "<connector-name>",
            "connection":{
                "class": "VirtualConnection",
                "connectorType" : {
                    "class": "ConnectorType",
                    "connectorProviderClassName": "org.odpi.openmetadata.adapters.connectors.integration.jdbc.JdbcIntegrationConnectorProvider"
                },
                "embeddedConnections":[
                    {
                        "class" : "EmbeddedConnection",
                        "embeddedConnection" : {
                            "class" : "Connection",
                            "userId" : "<user>",
                            "clearPassword" : "<password>",
                            "connectorType" : {
                                "class": "ConnectorType",
                                "connectorProviderClassName": "org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcConnectorProvider"
                            },
                            "endpoint":{
                                "class": "Endpoint",
                                "address" : "<jdbc-format-database-address>"
                            },
                            "configurationProperties" :{
                                "connectorTypeQualifiedName" : "<already-stored-connector-type>"
                            }
                        }
                    }
                ]
            },  
            "metadataSourceQualifiedName": "Source",
            "refreshTimeInterval": "60", 
            "usesBlockingCalls": "false",
            "permittedSynchronization": "FROM_THIRD_PARTY"
        }
    ]
}
```

**access-service-omag-url** - url of omag server that hosts the paired access service

**omas-server-name** - name of paired access server

**connector-name** - this connectors name

**already-stored-connector-type** - optional, qualified name of an already stored connector type. Necessary to create the connection structure supporting the database asset. Can be created via metadata archives 

**user** - database user

**password** - database password

**jdbc-format-database-address** - database address


