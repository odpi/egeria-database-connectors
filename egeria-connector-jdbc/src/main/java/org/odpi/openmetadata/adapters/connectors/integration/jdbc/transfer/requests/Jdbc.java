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

public class Jdbc {

    private final JdbcMetadata jdbcMetadata;
    private final AuditLog auditLog;

    public Jdbc(JdbcMetadata jdbcMetadata, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.auditLog = auditLog;
    }

    public String getConnectorTypeQualifiedName(){
        return new JdbcGetConnectorTypeQualifiedName(jdbcMetadata).get();
    }

    public String getUserName(){
        return new JdbcGetUserName(jdbcMetadata, auditLog).get();
    }

    public String getUrl(){
        return new JdbcGetUrl(jdbcMetadata, auditLog).get();
    }

    public String getDriverName(){
        return new JdbcGetDriverName(jdbcMetadata, auditLog).get();
    }

    public String getDatabaseProductVersion(){
        return new JdbcGetDatabaseProductVersion(jdbcMetadata, auditLog).get();
    }

    public String getDatabaseProductName(){
        return new JdbcGetDatabaseProductName(jdbcMetadata, auditLog).get();
    }

    public List<JdbcTable> getTables(String schemaName){
        return new JdbcGetTables(jdbcMetadata, auditLog).apply(schemaName);
    }

    public List<JdbcForeignKey> getImportedKeys(String schemaName, String tableName){
        return new JdbcGetImportedKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    public List<JdbcForeignKey> getExportedKeys(String schemaName, String tableName){
        return new JdbcGetExportedKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    public List<JdbcPrimaryKey> getPrimaryKeys(String schemaName, String tableName){
        return new JdbcGetPrimaryKeys(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    public List<JdbcColumn> getColumns(String schemaName, String tableName){
        return new JdbcGetColumns(jdbcMetadata, auditLog).apply(schemaName, tableName);
    }

    public List<JdbcSchema> getSchemas(){
        return new JdbcGetSchemas(jdbcMetadata, auditLog).get();
    }

}
