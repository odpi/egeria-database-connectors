/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Jdbc;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcForeignKey;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcPrimaryKey;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_METADATA_TRANSFER;

/**
 * Transfers metadata from jdbc in an exploratory way. What can be accessed will be transferred. Triggers schema, table
 * and column metadata transfer
 */
public class JdbcMetadataTransfer {

    private final Jdbc jdbc;
    private final Omas omas;
    private final AuditLog auditLog;

    public JdbcMetadataTransfer(JdbcMetadata jdbcMetadata, DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog) {
        this.jdbc = new Jdbc(jdbcMetadata, auditLog);
        this.omas = new Omas(databaseIntegratorContext, auditLog);
        this.auditLog = auditLog;
    }

    public boolean execute() {
        String methodName = "JdbcMetadataTransfer.execute";

        DatabaseElement database = new DatabaseTransfer(jdbc, omas, auditLog).execute();
        if (database == null) {
            auditLog.logMessage("No database metadata transferred. Exiting",
                    EXITING_ON_METADATA_TRANSFER.getMessageDefinition(methodName));
            return false;
        }

        createAssetConnection(database);
        transferSchemas(database);

        List<DatabaseSchemaElement> schemas = omas.getSchemas(database.getElementHeader().getGUID());
        if(schemas.isEmpty()){
            auditLog.logMessage("No schema metadata transferred. Exiting",
                    EXITING_ON_METADATA_TRANSFER.getMessageDefinition(methodName));
            return false;
        }

        transferTables(schemas);
        transferColumns(schemas);
        transferForeignKeys(database);
        return true;

    }

    private void createAssetConnection(DatabaseElement databaseElement){
        CreateConnectionStructure createConnectionStructure = new CreateConnectionStructure(omas, jdbc, auditLog);
        createConnectionStructure.accept(databaseElement);
    }

    private void transferSchemas(DatabaseElement databaseElement){
        String databaseQualifiedName = databaseElement.getDatabaseProperties().getQualifiedName();
        String databaseGuid = databaseElement.getElementHeader().getGUID();

        List<DatabaseSchemaElement> omasSchemas = omas.getSchemas(databaseGuid);
        List<DatabaseSchemaElement> omasSchemasUpdated =
                jdbc.getSchemas().stream().map(new SchemaTransfer(omas, auditLog, omasSchemas, databaseQualifiedName, databaseGuid))
                        .collect(Collectors.toList());

        omasSchemas.removeAll(omasSchemasUpdated);
        omasSchemas.forEach(omas::removeSchema);
    }

    private void transferTables(List<DatabaseSchemaElement> schemas){
        schemas.forEach( schema -> {
            String schemaDisplayName = schema.getDatabaseSchemaProperties().getDisplayName();
            String schemaGuid = schema.getElementHeader().getGUID();
            String schemaQualifiedName = schema.getDatabaseSchemaProperties().getQualifiedName();
            List<DatabaseTableElement> omasTables = omas.getTables(schemaGuid);

            List<DatabaseTableElement> omasTablesUpdated = jdbc.getTables(schemaDisplayName).stream()
                    .map(new TableTransfer(omas, auditLog, omasTables, schemaQualifiedName, schemaGuid))
                    .collect(Collectors.toList());

            omasTables.removeAll(omasTablesUpdated);
            omasTables.forEach(omas::removeTable);
        });
    }

    private void transferColumns(List<DatabaseSchemaElement> schemas){
        schemas.forEach(schema -> {
            List<DatabaseTableElement> tables = omas.getTables(schema.getElementHeader().getGUID());

            tables.forEach( table ->{
                String schemaName = table.getDatabaseTableProperties().getQualifiedName().split("::")[1];
                String tableName = table.getDatabaseTableProperties().getDisplayName();
                String tableGuid = table.getElementHeader().getGUID();

                List<JdbcPrimaryKey> jdbcPrimaryKeys = jdbc.getPrimaryKeys(schemaName, tableName);
                List<DatabaseColumnElement> omasColumns = omas.getColumns(tableGuid);

                List<DatabaseColumnElement> omasUpdatedColumns = jdbc.getColumns(schemaName, tableName).stream()
                        .map(new ColumnTransfer(omas, auditLog, omasColumns, jdbcPrimaryKeys, table)).collect(Collectors.toList());

                omasColumns.removeAll(omasUpdatedColumns);
                omasColumns.forEach(omas::removeColumn);
            });
        });
    }

    private void transferForeignKeys(DatabaseElement database){
        Set<JdbcForeignKey> foreignKeys = Stream.concat(
                jdbc.getSchemas().stream()
                        .flatMap(s -> jdbc.getTables(s.getTableSchem()).stream())
                        .flatMap(t -> jdbc.getImportedKeys(t.getTableSchem(), t.getTableName()).stream()),
                jdbc.getSchemas().stream()
                        .flatMap(s -> jdbc.getTables(s.getTableSchem()).stream())
                        .flatMap(t -> jdbc.getExportedKeys(t.getTableSchem(), t.getTableName()).stream())
        ).collect(Collectors.toSet());

        foreignKeys.forEach(new ForeignKeyTransfer(omas, auditLog, database));

    }

}
