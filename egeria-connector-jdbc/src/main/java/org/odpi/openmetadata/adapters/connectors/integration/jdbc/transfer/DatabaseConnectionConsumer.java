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
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_JDBC;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_UPSERTING_INTO_OMAS;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_WHEN_SETTING_ASSET_CONNECTION;

class DatabaseConnectionConsumer implements Consumer<DatabaseElement> {

    private final DatabaseIntegratorContext databaseIntegratorContext;
    private final AuditLog auditLog;
    private final JdbcMetadata jdbcMetadata;

    DatabaseConnectionConsumer(DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog, JdbcMetadata jdbcMetadata){
        this.databaseIntegratorContext = databaseIntegratorContext;
        this.auditLog = auditLog;
        this.jdbcMetadata = jdbcMetadata;
    }

    @Override
    public void accept(DatabaseElement databaseElement) {
        String methodName = "createAssetConnection";

        String connectorTypeQualifiedName = jdbcMetadata.getConnectorTypeQualifiedName();
        if(StringUtils.isBlank(connectorTypeQualifiedName)){
            auditLog.logMessage("Missing connector type qualified name. Skipping asset connection setup",
                    null);
            return;
        }
        String connectorTypeGuid = determineConnectorTypeGuid(connectorTypeQualifiedName);
        if(StringUtils.isBlank(connectorTypeGuid)){
            auditLog.logMessage("Missing connector type guid. Skipping asset connection setup",
                    null);
            return;
        }

        String databaseGuid = databaseElement.getElementHeader().getGUID();
        if(StringUtils.isBlank(databaseGuid)){
            auditLog.logMessage("Missing database guid. Skipping asset connection setup",
                    null);
            return;
        }

        ConnectionProperties connectionProperties = createConnectionProperties(databaseElement);
        String connectionGuid = determineConnectionGuid(connectionProperties);
        if(StringUtils.isBlank(connectionGuid)){
            auditLog.logMessage("Missing connection guid. Skipping asset connection setup",
                    null);
            return;
        }

        EndpointProperties endpointProperties = createEndpointProperties(connectionProperties);
        String endpointGuid = determineEndpointGuid(endpointProperties);
        if(StringUtils.isBlank(endpointGuid)){
            auditLog.logMessage("Missing endpoint guid. Skipping asset connection setup",
                    null);
            return;
        }

        try {
            databaseIntegratorContext.setupConnectorType(connectionGuid, connectorTypeGuid);
            databaseIntegratorContext.setupAssetConnection(databaseGuid,
                    databaseElement.getDatabaseProperties().getDescription(), connectionGuid);
            databaseIntegratorContext.setupEndpoint(connectionGuid, endpointGuid);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logMessage("Setting up connection (guid: " + connectionGuid
                            + "), connector type (guid: " + connectorTypeQualifiedName
                            + ") and asset (guid: " + databaseGuid + " . Ignoring",
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()));
        }
    }

    private ConnectionProperties createConnectionProperties(DatabaseElement databaseElement){
        ConnectionProperties connectionProperties = new ConnectionProperties();
        connectionProperties.setDisplayName(databaseElement.getDatabaseProperties().getDisplayName() + " Connection");
        connectionProperties.setQualifiedName(databaseElement.getDatabaseProperties().getQualifiedName() + "::connection");
        connectionProperties.setConfigurationProperties(databaseElement.getDatabaseProperties().getExtendedProperties());

        return connectionProperties;
    }

    private String determineConnectionGuid(ConnectionProperties connectionProperties){
        String methodName = "determineConnectionGuid";
        try {
            Optional<List<ConnectionElement>> connections = Optional.ofNullable(
                    databaseIntegratorContext.getConnectionsByName(connectionProperties.getQualifiedName(),
                            0, 0));
            if(connections.isPresent()){
                if(connections.get().size() == 1){
                    return connections.get().get(0).getElementHeader().getGUID();
                }
            }else{
                return databaseIntegratorContext.createConnection(connectionProperties);
            }
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logMessage("Determining connection guid",
                    ERROR_WHEN_SETTING_ASSET_CONNECTION.getMessageDefinition(methodName));
        }

        return null;
    }

    private String determineConnectorTypeGuid(String connectorTypeQualifiedName){
        String methodName = "determineConnectorTypeGuid";
        try{
            Optional<List<ConnectorTypeElement>> connectorTypes = Optional.ofNullable(
                    databaseIntegratorContext.getConnectorTypesByName(connectorTypeQualifiedName, 0, 0));
            if(connectorTypes.isPresent()){
                if(connectorTypes.get().size() == 1){
                    return connectorTypes.get().get(0).getElementHeader().getGUID();
                }
            }
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logMessage("Determining connector type guid",
                    ERROR_WHEN_SETTING_ASSET_CONNECTION.getMessageDefinition(methodName));
        }
        return null;
    }

    private EndpointProperties createEndpointProperties(ConnectionProperties connectionProperties){
        String methodName = "createEndpointProperties";

        EndpointProperties endpointProperties = new EndpointProperties();
        endpointProperties.setDisplayName(connectionProperties.getDisplayName() + " Endpoint");
        endpointProperties.setQualifiedName(connectionProperties.getQualifiedName()+"::endpoint");
        try {
            endpointProperties.setAddress(jdbcMetadata.getUrl());
        } catch (SQLException sqlException) {
            auditLog.logMessage("Reading url from jdbc metadata",
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()));
        }

        return endpointProperties;
    }

    private String determineEndpointGuid(EndpointProperties endpointProperties){
        String methodName = "determineEndpointGuid";
        try{
            Optional<List<EndpointElement>> endpoints = Optional.ofNullable(
                    databaseIntegratorContext.findEndpoints(endpointProperties.getQualifiedName(), 0, 0));
            if(endpoints.isPresent()){
                if(endpoints.get().size() == 1) {
                    return endpoints.get().get(0).getElementHeader().getGUID();
                }
            }else{
                return databaseIntegratorContext.createEndpoint(endpointProperties);
            }
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logMessage("Determining endpoint guid",
                    ERROR_WHEN_SETTING_ASSET_CONNECTION.getMessageDefinition(methodName));
        }
        return null;
    }

}
