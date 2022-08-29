/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.apache.commons.lang3.StringUtils;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.ConnectionElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.ConnectorTypeElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.EndpointElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.ConnectionProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.EndpointProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Jdbc;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;
import java.util.function.Consumer;

class CreateConnectionStructure implements Consumer<DatabaseElement> {

    private final Omas omas;
    private final Jdbc jdbc;
    private final AuditLog auditLog;

    CreateConnectionStructure(Omas omas, Jdbc jdbc, AuditLog auditLog) {
        this.omas = omas;
        this.jdbc = jdbc;
        this.auditLog = auditLog;
    }

    @Override
    public void accept(DatabaseElement databaseElement) {
        String connectorTypeQualifiedName = jdbc.getConnectorTypeQualifiedName();
        if(StringUtils.isBlank(connectorTypeQualifiedName)){
            auditLog.logMessage("Missing connector type qualified name. Skipping asset connection setup", null);
            return;
        }
        String connectorTypeGuid = determineConnectorTypeGuid(connectorTypeQualifiedName);
        if(StringUtils.isBlank(connectorTypeGuid)){
            auditLog.logMessage("Missing connector type guid. Skipping asset connection setup", null);
            return;
        }

        String databaseGuid = databaseElement.getElementHeader().getGUID();
        if(StringUtils.isBlank(databaseGuid)){
            auditLog.logMessage("Missing database guid. Skipping asset connection setup", null);
            return;
        }

        ConnectionProperties connectionProperties = createConnectionProperties(databaseElement);
        String connectionGuid = determineConnectionGuid(connectionProperties);
        if(StringUtils.isBlank(connectionGuid)){
            auditLog.logMessage("Missing connection guid. Skipping asset connection setup", null);
            return;
        }

        EndpointProperties endpointProperties = createEndpointProperties(connectionProperties);
        String endpointGuid = determineEndpointGuid(endpointProperties);
        if(StringUtils.isBlank(endpointGuid)){
            auditLog.logMessage("Missing endpoint guid. Skipping asset connection setup",
                    null);
            return;
        }

        omas.setupConnectorType(connectionGuid, connectorTypeGuid);
        omas.setupAssetConnection(databaseGuid, databaseElement.getDatabaseProperties().getDescription(), connectionGuid);
        omas.setupEndpoint(connectionGuid, endpointGuid);
    }

    private ConnectionProperties createConnectionProperties(DatabaseElement databaseElement){
        ConnectionProperties connectionProperties = new ConnectionProperties();
        connectionProperties.setDisplayName(databaseElement.getDatabaseProperties().getDisplayName() + " Connection");
        connectionProperties.setQualifiedName(databaseElement.getDatabaseProperties().getQualifiedName() + "::connection");
        connectionProperties.setConfigurationProperties(databaseElement.getDatabaseProperties().getExtendedProperties());

        return connectionProperties;
    }

    private String determineConnectionGuid(ConnectionProperties connectionProperties){
        List<ConnectionElement> connections = omas.getConnectionsByName(connectionProperties.getQualifiedName());
        if(connections.isEmpty()){
            return omas.createConnection(connectionProperties).orElse("");
        }else{
            if(connections.size() == 1){
                return connections.get(0).getElementHeader().getGUID();
            }
        }

        return null;
    }

    private String determineConnectorTypeGuid(String connectorTypeQualifiedName){
        List<ConnectorTypeElement> connectorTypes = omas.getConnectorTypesByName(connectorTypeQualifiedName);
        if(connectorTypes.size() == 1){
            return connectorTypes.get(0).getElementHeader().getGUID();
        }
        return null;
    }

    private EndpointProperties createEndpointProperties(ConnectionProperties connectionProperties){
        EndpointProperties endpointProperties = new EndpointProperties();
        endpointProperties.setDisplayName(connectionProperties.getDisplayName() + " Endpoint");
        endpointProperties.setQualifiedName(connectionProperties.getQualifiedName()+"::endpoint");
        endpointProperties.setAddress(jdbc.getUrl());

        return endpointProperties;
    }

    private String determineEndpointGuid(EndpointProperties endpointProperties){
        List<EndpointElement> endpoints = omas.findEndpoints(endpointProperties.getQualifiedName());
        if(endpoints.isEmpty()){
            return omas.createEndpoint(endpointProperties).orElse("");
        }else{
            if(endpoints.size() == 1) {
                return endpoints.get(0).getElementHeader().getGUID();
            }
        }
        return null;
    }

}
