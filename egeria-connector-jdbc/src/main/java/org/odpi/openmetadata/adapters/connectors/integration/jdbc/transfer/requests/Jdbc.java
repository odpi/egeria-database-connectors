/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcForeignKey;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcPrimaryKey;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcTable;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;

/**
 * Utility class that delegates requests to jdbc 
 */
public class Jdbc {

    private final JdbcMetadata jdbcMetadata;
    private final AuditLog auditLog;

    public Jdbc(JdbcMetadata jdbcMetadata, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.auditLog = auditLog;
    }

    /**
     * Get connector type qualified name
     *
     * @return connector type qualified name
     */
    public String getConnectorTypeQualifiedName(){
        return new JdbcGetConnectorTypeQualifiedName(jdbcMetadata).get();
    }

    /**
     * Get user name
     *
     * @return user name
     */
    public String getUserName(){
        return new JdbcGetUserName(jdbcMetadata, auditLog).get();
    }

    /**
     * Get url
     *
     * @return url
     */
    public String getUrl(){
        return new JdbcGetUrl(jdbcMetadata, auditLog).get();
    }

    /**
     * Get driver name
     *
     * @return driver name
     */
    public String getDriverName(){
        return new JdbcGetDriverName(jdbcMetadata, auditLog).get();
    }

    /**
     * Get database product version
     *
     * @return database product version
     */
    public String getDatabaseProductVersion(){
        return new JdbcGetDatabaseProductVersion(jdbcMetadata, auditLog).get();
    }

    /**
     * Get database product name
     *
     * @return database product name
     */
    public String getDatabaseProductName(){
        return new JdbcGetDatabaseProductName(jdbcMetadata, auditLog).get();
    }

    /**
     * Get all tables of a schema
     *
     * @param schemaName schema name
     *
     * @return tables
     */
    public List<JdbcTable> getTables(String schemaName){
        return new JdbcGetTables(jdbcMetadata, auditLog).apply(schemaName);
    }

    /**
     * Get foreign keys as described by the primary key columns referenced by foreign key columns of target table
     *
     * @param schemaName schema name
     * @param tableName table name
     *
     * @return foreign keys
     */
    public List<JdbcForeignKey> getImportedKeys(String schemaName, String tableName){
        return new JdbcGetImportedKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    /**
     * Get foreign keys as described by the foreign key columns referenced by primary key columns of target table
     *
     * @param schemaName schema name
     * @param tableName table name
     *
     * @return foreign keys
     */
    public List<JdbcForeignKey> getExportedKeys(String schemaName, String tableName){
        return new JdbcGetExportedKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    /**
     * Get table primary keys
     *
     * @param schemaName schema name
     * @param tableName table name
     *
     * @return primary keys
     */
    public List<JdbcPrimaryKey> getPrimaryKeys(String schemaName, String tableName){
        return new JdbcGetPrimaryKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    /**
     * Get all column of table
     *
     * @param schemaName schema name
     * @param tableName table name
     *
     * @return columns
     */
    public List<JdbcColumn> getColumns(String schemaName, String tableName){
        return new JdbcGetColumns(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    /**
     * Get all schemas
     *
     * @return schemas
     */
    public List<JdbcSchema> getSchemas(){
        return new JdbcGetSchemas(jdbcMetadata, auditLog).get();
    }

}
