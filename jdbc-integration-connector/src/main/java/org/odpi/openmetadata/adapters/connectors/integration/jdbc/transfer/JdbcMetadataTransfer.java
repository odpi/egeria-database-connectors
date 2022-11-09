/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.model.JdbcForeignKey;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.model.JdbcPrimaryKey;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Jdbc;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_DATABASE_TRANSFER_FAIL;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS;

/**
 * Transfers metadata from jdbc in an exploratory way. What can be accessed will be transferred
 */
public class JdbcMetadataTransfer {

    private final Jdbc jdbc;
    private final Omas omas;
    private final String connectorTypeQualifiedName;
    private final AuditLog auditLog;

    public JdbcMetadataTransfer(JdbcMetadata jdbcMetadata, DatabaseIntegratorContext databaseIntegratorContext,
                                String connectorTypeQualifiedName, AuditLog auditLog) {
        this.jdbc = new Jdbc(jdbcMetadata, auditLog);
        this.omas = new Omas(databaseIntegratorContext, auditLog);
        this.connectorTypeQualifiedName = connectorTypeQualifiedName;
        this.auditLog = auditLog;
    }

    /**
     * Triggers database, schema, table and column metadata transfer. Will do the best it can to transfer as much of the
     * metadata as possible. If available will also build the asset (database) connection structure
     */
    public void execute() {
        String methodName = "JdbcMetadataTransfer.execute";

        DatabaseElement database = new DatabaseTransfer(jdbc, omas, auditLog).execute();
        if (database == null) {
            auditLog.logMessage("Verifying database metadata transferred. None found. Stopping transfer",
                    EXITING_ON_DATABASE_TRANSFER_FAIL.getMessageDefinition(methodName));
            return;
        }

        createAssetConnection(database);

        transferTablesWithoutSchema(database);
        transferColumnsOfTablesWithoutSchema(database);
        transferForeignKeysIgnoringSchemas(database);

        transferSchemas(database);
        List<DatabaseSchemaElement> schemas = omas.getSchemas(database.getElementHeader().getGUID());
        if(schemas.isEmpty()){
            return;
        }
        transferTables(database, schemas);
        transferColumns(database, schemas);
        transferForeignKeys(database);
    }

    /**
     * Triggers the transfer of available tables that are not assigned to any schema
     *
     * @param databaseElement database element
     */
    private void transferTablesWithoutSchema(DatabaseElement databaseElement) {
        long start = System.currentTimeMillis();

        String databaseQualifiedName = databaseElement.getDatabaseProperties().getQualifiedName();
        String databaseGuid = databaseElement.getElementHeader().getGUID();
        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        // already known tables by the omas, previously transferred
        List<DatabaseTableElement> omasTables = omas.getTables(databaseGuid);
        // a table update will always occur as long as the table is returned by jdbc
        List<DatabaseTableElement> omasTablesUpdated = jdbc.getTables(catalog,"").parallelStream()
                .filter(jdbcTable -> jdbcTable.getTableSchem() == null || jdbcTable.getTableSchem().length() < 1 )
                .map(new TableTransfer(omas, auditLog, omasTables, databaseQualifiedName, databaseGuid))
                .collect(Collectors.toList());

        // will remove all updated tables, and what remains are the ones deleted in jdbc
        omasTables.removeAll(omasTablesUpdated);
        // remove from omas the tables deleted in jdbc
        omasTables.forEach(omas::removeTable);

        long end = System.currentTimeMillis();
        auditLog.logMessage("Transferring tables without schema",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("tables with no schema", "" + (end - start)/1000));
    }

    /**
     * Triggers the transfer of columns from tables without schema
     *
     * @param databaseElement database element
     */
    private void transferColumnsOfTablesWithoutSchema(DatabaseElement databaseElement){
        long start = System.currentTimeMillis();

        String databaseGuid = databaseElement.getElementHeader().getGUID();
        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        omas.getTables(databaseGuid).parallelStream()
                .peek(table -> {
                    String schemaName = "";
                    String tableName = table.getDatabaseTableProperties().getDisplayName();
                    String tableGuid = table.getElementHeader().getGUID();

                    List<JdbcPrimaryKey> jdbcPrimaryKeys = jdbc.getPrimaryKeys(schemaName, tableName);
                    // already known columns by the omas, previously transferred
                    List<DatabaseColumnElement> omasColumns = omas.getColumns(tableGuid);
                    // a column update will always occur as long as the column is returned by jdbc
                    List<DatabaseColumnElement> omasUpdatedColumns = jdbc.getColumns(catalog, schemaName, tableName).parallelStream()
                            .map(new ColumnTransfer(omas, auditLog, omasColumns, jdbcPrimaryKeys, table)).collect(Collectors.toList());

                    // will remove all updated column, and what remains are the ones deleted in jdbc
                    omasColumns.removeAll(omasUpdatedColumns);
                    // remove from omas the columns deleted in jdbc
                    omasColumns.forEach(omas::removeColumn);
                }).collect(Collectors.toList());

        long end = System.currentTimeMillis();
        auditLog.logMessage("Transferring columns of tables without schema",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("columns of tables with no schema", "" + (end - start)/1000));
    }

    /**
     * Triggers the transfer of all foreign keys between columns of tables without schemas
     *
     * @param databaseElement database element
     */
    private void transferForeignKeysIgnoringSchemas(DatabaseElement databaseElement){
        long start = System.currentTimeMillis();

        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        Set<JdbcForeignKey> foreignKeys = Stream.concat(
                        jdbc.getTables(catalog, "").stream()
                                .flatMap( t -> jdbc.getImportedKeys(catalog, "", t.getTableName()).stream() ),
                        jdbc.getTables(catalog, "").stream()
                                .flatMap( t -> jdbc.getExportedKeys(catalog, "", t.getTableName()).stream() ))
                .collect(Collectors.toSet());

        foreignKeys.forEach(new ForeignKeyTransfer(omas, auditLog, databaseElement));

        long end = System.currentTimeMillis();
        auditLog.logMessage("Foreign key transfer complete",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS
                        .getMessageDefinition("foreign keys between columns of tables without schemas", "" + (end - start)/1000));
    }

    private void createAssetConnection(DatabaseElement databaseElement){
        CreateConnectionStructure createConnectionStructure = new CreateConnectionStructure(omas, jdbc,
                connectorTypeQualifiedName, auditLog);
        createConnectionStructure.accept(databaseElement);
    }

    /**
     * Triggers the transfer of all available schemas
     *
     * @param databaseElement database
     */
    private void transferSchemas(DatabaseElement databaseElement){
        long start = System.currentTimeMillis();

        String databaseQualifiedName = databaseElement.getDatabaseProperties().getQualifiedName();
        String databaseGuid = databaseElement.getElementHeader().getGUID();
        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        // already known schemas by the omas, previously transferred
        List<DatabaseSchemaElement> omasSchemas = omas.getSchemas(databaseGuid);
        // a schema update will always occur as long as the schema is returned by jdbc
        List<DatabaseSchemaElement> omasSchemasUpdated =
                jdbc.getSchemas(catalog).parallelStream().map(new SchemaTransfer(omas, auditLog, omasSchemas, databaseQualifiedName, databaseGuid))
                        .collect(Collectors.toList());

        // will remove all updated schemas, and what remains are the ones deleted in jdbc
        omasSchemas.removeAll(omasSchemasUpdated);
        // remove from omas the schemas deleted in jdbc
        omasSchemas.forEach(omas::removeSchema);

        long end = System.currentTimeMillis();
        auditLog.logMessage("Schema transfer complete",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("schemas", "" + (end - start)/1000));
    }

    /**
     * Triggers the transfer of all available tables
     *
     * @param databaseElement database element
     * @param schemas schemas
     */
    private void transferTables(DatabaseElement databaseElement, List<DatabaseSchemaElement> schemas){
        long start = System.currentTimeMillis();

        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        schemas.parallelStream().peek( schema -> {
            String schemaDisplayName = schema.getDatabaseSchemaProperties().getDisplayName();
            String schemaGuid = schema.getElementHeader().getGUID();
            String schemaQualifiedName = schema.getDatabaseSchemaProperties().getQualifiedName();

            // already known tables by the omas, previously transferred
            List<DatabaseTableElement> omasTables = omas.getTables(schemaGuid);
            // a table update will always occur as long as the table is returned by jdbc
            List<DatabaseTableElement> omasTablesUpdated = jdbc.getTables(catalog, schemaDisplayName).parallelStream()
                    .map(new TableTransfer(omas, auditLog, omasTables, schemaQualifiedName, schemaGuid))
                    .collect(Collectors.toList());

            // will remove all updated tables, and what remains are the ones deleted in jdbc
            omasTables.removeAll(omasTablesUpdated);
            // remove from omas the tables deleted in jdbc
            omasTables.forEach(omas::removeTable);
        }).collect(Collectors.toList());

        long end = System.currentTimeMillis();
        auditLog.logMessage("Table transfer complete",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("tables", "" + (end - start)/1000));
    }

    /**
     * Triggers the transfer of all available tables
     *
     * @param databaseElement database element
     * @param schemas schemas
     */
    private void transferColumns(DatabaseElement databaseElement, List<DatabaseSchemaElement> schemas){
        long start = System.currentTimeMillis();

        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

         schemas.parallelStream()
                 .flatMap(s -> omas.getTables(s.getElementHeader().getGUID()).parallelStream())
                 .peek(table -> {
                     String schemaName = table.getDatabaseTableProperties().getQualifiedName().split("::")[1];
                     String tableName = table.getDatabaseTableProperties().getDisplayName();
                     String tableGuid = table.getElementHeader().getGUID();

                     List<JdbcPrimaryKey> jdbcPrimaryKeys = jdbc.getPrimaryKeys(schemaName, tableName);
                     // already known columns by the omas, previously transferred
                     List<DatabaseColumnElement> omasColumns = omas.getColumns(tableGuid);
                     // a column update will always occur as long as the column is returned by jdbc
                     List<DatabaseColumnElement> omasUpdatedColumns = jdbc.getColumns(catalog, schemaName, tableName).parallelStream()
                             .map(new ColumnTransfer(omas, auditLog, omasColumns, jdbcPrimaryKeys, table)).collect(Collectors.toList());

                     // will remove all updated column, and what remains are the ones deleted in jdbc
                     omasColumns.removeAll(omasUpdatedColumns);
                     // remove from omas the columns deleted in jdbc
                     omasColumns.forEach(omas::removeColumn);
                }).collect(Collectors.toList());

        long end = System.currentTimeMillis();
        auditLog.logMessage("Column transfer complete",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("columns", "" + (end - start)/1000));
    }

    /**
     * Triggers the transfer of all foreign keys. The reason for doing this at database level is that a foreign key relationship
     * can exist between columns located in tables in different schemas
     *
     * @param databaseElement database element
     */
    private void transferForeignKeys(DatabaseElement databaseElement){
        long start = System.currentTimeMillis();

        String catalog = databaseElement.getDatabaseProperties().getDisplayName();

        // all foreign keys as returned by calling getExportedKeys and getImportedKeys on jdbc
        Set<JdbcForeignKey> foreignKeys = Stream.concat(
                jdbc.getSchemas(catalog).stream()
                        .flatMap(s -> jdbc.getTables(catalog, s.getTableSchem()).stream())
                        .flatMap(t -> jdbc.getImportedKeys(catalog, t.getTableSchem(), t.getTableName()).stream()),
                jdbc.getSchemas(catalog).stream()
                        .flatMap(s -> jdbc.getTables(catalog, s.getTableSchem()).stream())
                        .flatMap(t -> jdbc.getExportedKeys(catalog, t.getTableSchem(), t.getTableName()).stream())
        ).collect(Collectors.toSet());

        foreignKeys.forEach(new ForeignKeyTransfer(omas, auditLog, databaseElement));

        long end = System.currentTimeMillis();
        auditLog.logMessage("Foreign key transfer complete",
                PARTIAL_TRANSFER_COMPLETE_FOR_DB_OBJECTS.getMessageDefinition("foreign keys", "" + (end - start)/1000));
    }

}
