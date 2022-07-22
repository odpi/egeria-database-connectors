/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseColumnProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseTableProperties;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_OMAS;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_JDBC;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_UPSERTING_INTO_OMAS;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_METADATA_TRANSFER;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.UNKNOWN_ERROR_WHILE_METADATA_TRANSFER;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcTable;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class JdbcMetadataTransfer {

    private final JdbcMetadata jdbcMetadata;
    private final DatabaseIntegratorContext databaseIntegratorContext;
    private final AuditLog auditLog;

    private final RemoveDatabaseSchemaConsumer removeDatabaseSchemaConsumer;
    private final RemoveDatabaseTableConsumer removeDatabaseTableConsumer;
    private final RemoveDatabaseColumnConsumer removeDatabaseColumnConsumer;

    public JdbcMetadataTransfer(JdbcMetadata jdbcMetadata, DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.databaseIntegratorContext = databaseIntegratorContext;
        this.auditLog = auditLog;
        this.removeDatabaseSchemaConsumer = new RemoveDatabaseSchemaConsumer(databaseIntegratorContext);
        this.removeDatabaseTableConsumer = new RemoveDatabaseTableConsumer(databaseIntegratorContext);
        this.removeDatabaseColumnConsumer = new RemoveDatabaseColumnConsumer(databaseIntegratorContext);
    }

    public boolean execute() {
        String methodName = "execute";
        try {
            DatabaseElement databaseElement = transferDatabase();
            if (databaseElement == null) {
                auditLog.logMessage("No database metadata transferred. Exiting",
                        EXITING_ON_METADATA_TRANSFER.getMessageDefinition());
                return false;
            }

            List<DatabaseSchemaElement> schemas = transferSchemas(databaseElement);
            transferTables(schemas);
            return true;
        }catch (Exception e){
            auditLog.logException("Transferring metadata",
                    UNKNOWN_ERROR_WHILE_METADATA_TRANSFER.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return false;
    }

    private void transferTables(List<DatabaseSchemaElement> schemas) {
        for(DatabaseSchemaElement schemaElement : schemas){
            DatabaseSchemaProperties databaseSchemaProperties = schemaElement.getDatabaseSchemaProperties();

            List<JdbcTable> jdbcTables = getJdbcTables(databaseSchemaProperties.getDisplayName());
            List<DatabaseTableElement> omasTables = this.getOmasTables(schemaElement.getElementHeader().getGUID());

            for(JdbcTable jdbcTable : jdbcTables){
                DatabaseTableProperties jdbcTableProperties = new DatabaseTableProperties();
                jdbcTableProperties.setDisplayName(jdbcTable.getTableName());
                String databaseTableQualifiedName = databaseSchemaProperties.getQualifiedName() + "::" + jdbcTable.getTableName();
                jdbcTableProperties.setQualifiedName(databaseTableQualifiedName);

                Optional<DatabaseTableElement> omasTable = omasTables.stream()
                        .filter(dte -> dte.getDatabaseTableProperties().getQualifiedName().equals(databaseTableQualifiedName))
                        .findFirst();

                if(omasTable.isPresent()){
                    this.updateOmasTable(omasTable.get(), jdbcTableProperties);
                    omasTables.remove(omasTable.get());
                }else{
                    Optional<String> tableGuid = this.createOmasTable(schemaElement, jdbcTableProperties);
                    if(tableGuid.isPresent()){
                        omasTable = this.getOmasTable(tableGuid.get());
                    }else{
                        // move on to the next table, as something happened with saving the new table
                        continue;
                    }
                }
                omasTable.ifPresent(tableElement -> transferColumns(schemaElement, tableElement));
            }
            omasTables.forEach(removeDatabaseTableConsumer);
        }
    }

    private Optional<DatabaseTableElement> getOmasTable(String tableGuid){
        String methodName = "getDatabaseTable";
        try{
            return Optional.ofNullable(databaseIntegratorContext.getDatabaseTableByGUID(tableGuid));
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logException("Error reading table from OMAS for guid: " + tableGuid,
                    ERROR_READING_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return Optional.empty();
    }

    private Optional<String> createOmasTable(DatabaseSchemaElement omasSchema, DatabaseTableProperties newTableProperties){
        String methodName = "createDatabaseTable";

        try {
            return Optional.ofNullable(databaseIntegratorContext
                    .createDatabaseTable(omasSchema.getElementHeader().getGUID(), newTableProperties));
        } catch (InvalidParameterException | PropertyServerException | UserNotAuthorizedException e) {
            auditLog.logException("Error creating schema in OMAS: " + newTableProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return Optional.empty();
    }

    private void updateOmasTable(DatabaseTableElement omasTable, DatabaseTableProperties tableProperties){
        String methodName = "updateDatabaseTable";
        try {
            databaseIntegratorContext.updateDatabaseTable(omasTable.getElementHeader().getGUID(), tableProperties);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logException("Error updating table in OMAS for qualifiedName: " + tableProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
    }

    private List<JdbcTable> getJdbcTables(String schemaName) {
        String methodName = "getJdbcTables";
        try {
            return Optional.ofNullable(
                    jdbcMetadata.getTables(null, schemaName, null, new String[]{"TABLE"}))
                    .orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Error reading tables from JDBC for schema: " + schemaName,
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }

    private List<DatabaseTableElement> getOmasTables(String schemaGuid){
        String methodName = "getOmasTables";
        try{
            return Optional.ofNullable(databaseIntegratorContext
                    .getTablesForDatabaseSchema(schemaGuid, 0, 0)).orElseGet(ArrayList::new);
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logException("Error reading tables from OMAS for schemaGuid: " + schemaGuid,
                    ERROR_READING_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return new ArrayList<>();
    }

    private void transferColumns(DatabaseSchemaElement schemaElement, DatabaseTableElement tableElement) {
        String schemaElementName = schemaElement.getDatabaseSchemaProperties().getDisplayName();
        String tableElementName = tableElement.getDatabaseTableProperties().getDisplayName();
        List<JdbcColumn> jdbcColumns = this.getJdbcColumns(schemaElementName, tableElementName);
        List<DatabaseColumnElement> omasColumns = this.getOmasColumns(tableElement.getElementHeader().getGUID());

        for(JdbcColumn jdbcColumn : jdbcColumns){
            DatabaseColumnProperties databaseColumnProperties = new DatabaseColumnProperties();
            databaseColumnProperties.setDisplayName(jdbcColumn.getColumnName());
            String databaseColumnQualifiedName = tableElement.getDatabaseTableProperties().getQualifiedName()
                    + "::" + jdbcColumn.getColumnName();
            databaseColumnProperties.setQualifiedName(databaseColumnQualifiedName);
            databaseColumnProperties.setDataType(extractDataType(jdbcColumn.getDataType()));

            Optional<DatabaseColumnElement> omasColumn = omasColumns.stream()
                    .filter(dce -> dce.getDatabaseColumnProperties().getQualifiedName().equals(databaseColumnQualifiedName))
                    .findFirst();

            if(omasColumn.isPresent()){
                this.updateOmasColumn(omasColumn.get(), databaseColumnProperties);
                omasColumns.remove(omasColumn.get());
            }else{
                this.createOmasColumn(tableElement, databaseColumnProperties);
            }
        }
        omasColumns.forEach(removeDatabaseColumnConsumer);
    }

    private void updateOmasColumn(DatabaseColumnElement omasColumn, DatabaseColumnProperties columnProperties){
        String methodName = "updateDatabaseColumn";
        try {
            databaseIntegratorContext.updateDatabaseColumn(omasColumn.getElementHeader().getGUID(), columnProperties);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logException("Error updating column in OMAS for qualifiedName: " + columnProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
    }

    private void createOmasColumn(DatabaseTableElement tableElement, DatabaseColumnProperties newColumnProperties){
        String methodName = "createDatabaseColumn";
        try {
            databaseIntegratorContext.createDatabaseColumn(tableElement.getElementHeader().getGUID(), newColumnProperties);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logException("Error creating column in OMAS: " + newColumnProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
    }

    private List<DatabaseColumnElement> getOmasColumns(String tableGuid){
        String methodName = "getOmasColumns";
        try{
            return Optional.ofNullable(
                    databaseIntegratorContext.getColumnsForDatabaseTable(tableGuid, 0, 0))
                    .orElseGet(ArrayList::new);
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logException("Error reading columns from OMAS for table guid: " + tableGuid ,
                    ERROR_READING_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return new ArrayList<>();
    }

    private List<JdbcColumn> getJdbcColumns(String schemaName, String tableName){
        String methodName = "getJdbcColumns";
        try{
            return Optional.ofNullable(
                    jdbcMetadata.getColumns(null, schemaName, tableName, null))
                    .orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Error reading tables from JDBC for schema " + schemaName + " and table " + tableName,
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }

    private String extractDataType(int jdbcDataType){
        String dataType = "<unknown>";
        try {
            dataType = JDBCType.valueOf(jdbcDataType).getName();
        }catch(IllegalArgumentException iae){
            // do nothing
        }
        return dataType;
    }

    private List<DatabaseSchemaElement> transferSchemas(DatabaseElement databaseElement) {

        List<JdbcSchema> jdbcSchemas = this.getJdbcSchemas();
        List<DatabaseSchemaElement> omasSchemas = this.getOmasSchemas(databaseElement);

        for (JdbcSchema jdbcSchema : jdbcSchemas) {
            DatabaseSchemaProperties jdbcSchemaProperties = new DatabaseSchemaProperties();
            jdbcSchemaProperties.setDisplayName(jdbcSchema.getTableSchem());
            String databaseSchemaQualifiedName =
                    databaseElement.getDatabaseProperties().getQualifiedName() + "::" + jdbcSchema.getTableSchem();
            jdbcSchemaProperties.setQualifiedName(databaseSchemaQualifiedName);

            Optional<DatabaseSchemaElement> omasSchema = omasSchemas.stream()
                    .filter(dse -> dse.getDatabaseSchemaProperties().getQualifiedName().equals(databaseSchemaQualifiedName))
                    .findFirst();
            if (omasSchema.isPresent()) {
                updateOmasSchema(omasSchema.get(), jdbcSchemaProperties);
                omasSchemas.remove(omasSchema.get());
            } else {
                createOmasSchema(databaseElement, jdbcSchemaProperties);
            }
        }
        omasSchemas.forEach(removeDatabaseSchemaConsumer);

        return getOmasSchemas(databaseElement);

    }

    private void createOmasSchema(DatabaseElement databaseElement, DatabaseSchemaProperties newSchemaProperties){
        String methodName = "createDatabaseSchema";
        try {
            databaseIntegratorContext.createDatabaseSchema(databaseElement.getElementHeader().getGUID(),
                    newSchemaProperties);
        } catch (InvalidParameterException | PropertyServerException | UserNotAuthorizedException e) {
            auditLog.logException("Error creating schema in OMAS: " + newSchemaProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
    }

    private void updateOmasSchema(DatabaseSchemaElement omasSchema, DatabaseSchemaProperties schemaProperties){
        String methodName = "updateDatabaseSchema";
        try {
            databaseIntegratorContext.updateDatabaseSchema(omasSchema.getElementHeader().getGUID(), schemaProperties);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logException("Error updating schema in OMAS for qualifiedName: " + schemaProperties.getQualifiedName(),
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
    }

    private List<JdbcSchema> getJdbcSchemas(){
        String methodName = "getJdbcSchemas";
        try {
            return Optional.ofNullable(jdbcMetadata.getSchemas()).orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Error reading schemas from JDBC",
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }

    private List<DatabaseSchemaElement> getOmasSchemas(DatabaseElement databaseElement){
        String methodName = "getOmasSchemas";
        try{
            return Optional.ofNullable(
                    databaseIntegratorContext.getSchemasForDatabase(databaseElement.getElementHeader().getGUID(), 0, 0))
                    .orElseGet(ArrayList::new);
        } catch (UserNotAuthorizedException | InvalidParameterException | PropertyServerException e) {
            auditLog.logException("Error reading schemas from OMAS",
                    ERROR_READING_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return new ArrayList<>();
    }

    private DatabaseElement transferDatabase() {
        String methodName = "transferDatabase";

        try {
            DatabaseIntegratorContext context = databaseIntegratorContext;
            DatabaseProperties databaseProperties = buildDatabaseProperties();
            List<DatabaseElement> databasesInOmas = Optional.ofNullable(
                    context.getDatabasesByName(databaseProperties.getQualifiedName(), 0, 0))
                    .orElseGet(ArrayList::new);
            if (databasesInOmas.isEmpty()) {
                context.createDatabase(databaseProperties);
            } else {
                context.updateDatabase(databasesInOmas.get(0).getElementHeader().getGUID(), databaseProperties);
            }
            return context.getDatabasesByName(databaseProperties.getQualifiedName(), 0, 0).get(0);
        }catch (SQLException sqlException){
            auditLog.logException("Error reading database properties from JDBC",
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e){
            auditLog.logException("Error upserting entity into OMAS",
                    ERROR_UPSERTING_INTO_OMAS.getMessageDefinition(methodName, e.getMessage()), e);
        }
        return null;
    }

    private DatabaseProperties buildDatabaseProperties() throws SQLException {
        String user = jdbcMetadata.getUserName();
        String driverName = jdbcMetadata.getDriverName();
        String databaseProductVersion = jdbcMetadata.getDatabaseProductVersion();
        String databaseProductName = jdbcMetadata.getDatabaseProductName();
        String url = jdbcMetadata.getUrl();

        DatabaseProperties databaseProperties = new DatabaseProperties();
        databaseProperties.setQualifiedName(url);
        databaseProperties.setDisplayName(user);
        databaseProperties.setDatabaseInstance(driverName);
        databaseProperties.setDatabaseVersion(databaseProductVersion);
        databaseProperties.setDatabaseType(databaseProductName);
        databaseProperties.setDatabaseImportedFrom(url);

        return databaseProperties;
    }

}
