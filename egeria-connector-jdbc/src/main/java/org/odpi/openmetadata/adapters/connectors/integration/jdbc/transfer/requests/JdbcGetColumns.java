/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_JDBC;

/**
 * Manages the getColumns call to jdbc
 */
class JdbcGetColumns implements BiFunction<String, String, List<JdbcColumn>> {

    private final JdbcMetadata jdbcMetadata;
    private final AuditLog auditLog;

    JdbcGetColumns(JdbcMetadata jdbcMetadata, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.auditLog = auditLog;
    }

    /**
     * Get all columns from table
     *
     * @param schemaName schema
     * @param tableName table
     *
     * @return columns or empty list
     *
     * See {@link JdbcMetadata#getColumns(String, String, String, String)}
     */
    @Override
    public List<JdbcColumn> apply(String schemaName, String tableName){
        String methodName = "JdbcGetColumns";
        try{
            return Optional.ofNullable(
                    jdbcMetadata.getColumns(null, schemaName, tableName, null))
                    .orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Reading columns from JDBC for schema " + schemaName + " and table " + tableName,
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }

}