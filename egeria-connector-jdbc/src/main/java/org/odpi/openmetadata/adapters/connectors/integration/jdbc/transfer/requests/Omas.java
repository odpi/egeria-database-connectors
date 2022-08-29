/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.ConnectionElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.ConnectorTypeElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.EndpointElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.ConnectionProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseColumnProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseForeignKeyProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabasePrimaryKeyProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseTableProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.EndpointProperties;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.List;
import java.util.Optional;

public class Omas {

    private final DatabaseIntegratorContext databaseIntegratorContext;
    private final AuditLog auditLog;

    public Omas(DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog){
        this.databaseIntegratorContext = databaseIntegratorContext;
        this.auditLog = auditLog;
    }

    public Optional<DatabaseTableElement> getTable(String tableGuid){
        return new OmasGetTable(databaseIntegratorContext, auditLog).apply(tableGuid);
    }

    public List<DatabaseSchemaElement> getSchemas(String databaseGuid){
        return new OmasGetSchemas(databaseIntegratorContext, auditLog).apply(databaseGuid);
    }

    public List<DatabaseTableElement> getTables(String schemaGuid){
        return new OmasGetTables(databaseIntegratorContext, auditLog).apply(schemaGuid);
    }

    public List<DatabaseColumnElement> getColumns(String tableGuid){
        return new OmasGetColumns(databaseIntegratorContext, auditLog).apply(tableGuid);
    }

    public Optional<String> createEndpoint(EndpointProperties newEndpointProperties){
        return new OmasCreateEndpoint(databaseIntegratorContext, auditLog).apply(newEndpointProperties);
    }

    public Optional<String> createConnection(ConnectionProperties newConnectionProperties){
        return new OmasCreateConnection(databaseIntegratorContext, auditLog).apply(newConnectionProperties);
    }

    public Optional<String> createDatabase(DatabaseProperties newDatabaseProperties){
        return new OmasCreateDatabase(databaseIntegratorContext, auditLog).apply(newDatabaseProperties);
    }

    public Optional<String> createSchema(String schemaGuid, DatabaseSchemaProperties newSchemaProperties){
        return new OmasCreateSchema(databaseIntegratorContext, auditLog).apply(schemaGuid, newSchemaProperties);
    }

    public Optional<String> createTable(String schemaGuid, DatabaseTableProperties newTableProperties){
        return new OmasCreateTable(databaseIntegratorContext, auditLog).apply(schemaGuid, newTableProperties);
    }

    public Optional<String> createColumn(String tableGuid, DatabaseColumnProperties newColumnProperties){
        return new OmasCreateColumn(databaseIntegratorContext, auditLog).apply(tableGuid, newColumnProperties);
    }

    public void removeSchema(DatabaseSchemaElement databaseSchemaElement) {
        new OmasRemoveSchema(databaseIntegratorContext, auditLog).accept(databaseSchemaElement);
    }

    public void removeTable(DatabaseTableElement databaseTableElement) {
        new OmasRemoveTable(databaseIntegratorContext, auditLog).accept(databaseTableElement);
    }

    public void removeColumn(DatabaseColumnElement databaseColumnElement) {
        new OmasRemoveColumn(databaseIntegratorContext, auditLog).accept(databaseColumnElement);
    }

    public void updateDatabase(String databaseGuid, DatabaseProperties databaseProperties){
        new OmasUpdateDatabase(databaseIntegratorContext, auditLog).accept(databaseGuid, databaseProperties);
    }

    public void updateSchema(String schemaGuid, DatabaseSchemaProperties schemaProperties){
        new OmasUpdateSchema(databaseIntegratorContext, auditLog).accept(schemaGuid, schemaProperties);
    }

    public void updateTable(String tableGuid, DatabaseTableProperties tableProperties){
        new OmasUpdateTable(databaseIntegratorContext, auditLog).accept(tableGuid, tableProperties);
    }

    public void updateColumn(DatabaseColumnElement omasColumn, DatabaseColumnProperties columnProperties){
        new OmasUpdateColumn(databaseIntegratorContext, auditLog).accept(omasColumn, columnProperties);
    }

    public void setPrimaryKey(String columnGuid, DatabasePrimaryKeyProperties databasePrimaryKeyProperties) {
        new OmasSetPrimaryKey(databaseIntegratorContext, auditLog).accept(columnGuid, databasePrimaryKeyProperties);
    }

    public void setForeignKey(String primaryKeyColumnGuid, String foreignKeyColumnGuid, DatabaseForeignKeyProperties databaseForeignProperties) {
        new OmasSetForeignKey(databaseIntegratorContext, auditLog).accept(primaryKeyColumnGuid, foreignKeyColumnGuid, databaseForeignProperties);
    }

    public List<DatabaseElement> getDatabasesByName(String databaseQualifiedName){
        return new OmasGetDatabasesByName(databaseIntegratorContext, auditLog).apply(databaseQualifiedName);
    }

    public List<ConnectorTypeElement> getConnectorTypesByName(String connectorTypeQualifiedName){
        return new OmasGetConnectorTypesByName(databaseIntegratorContext, auditLog).apply(connectorTypeQualifiedName);
    }

    public List<ConnectionElement> getConnectionsByName(String connectionQualifiedName){
        return new OmasGetConnectionsByName(databaseIntegratorContext, auditLog).apply(connectionQualifiedName);
    }

    public List<EndpointElement> findEndpoints(String searchBy){
        return new OmasFindEndpoints(databaseIntegratorContext, auditLog).apply(searchBy);
    }

    public List<DatabaseColumnElement> findDatabaseColumns(String searchBy){
        return new OmasFindDatabaseColumns(databaseIntegratorContext, auditLog).apply(searchBy);
    }

    public void setupConnectorType(String connectionGuid, String connectorTypeGuid){
        new OmasSetupConnectorType(databaseIntegratorContext, auditLog).accept(connectionGuid, connectorTypeGuid);
    }

    public void setupAssetConnection(String assetGuid, String assetSummary, String connectionGuid){
        new OmasSetupAssetConnection(databaseIntegratorContext, auditLog).accept(assetGuid, assetSummary, connectionGuid);
    }

    public void setupEndpoint(String connectionGuid, String endpointGuid){
        new OmasSetupEndpoint(databaseIntegratorContext, auditLog).accept(connectionGuid, endpointGuid);
    }

}
