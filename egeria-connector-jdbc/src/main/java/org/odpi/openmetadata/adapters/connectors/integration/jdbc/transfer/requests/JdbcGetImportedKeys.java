/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcForeignKey;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_JDBC;

class JdbcGetImportedKeys implements BiFunction<String, String, List<JdbcForeignKey>> {

    private final JdbcMetadata jdbcMetadata;
    private final AuditLog auditLog;

    JdbcGetImportedKeys(JdbcMetadata jdbcMetadata, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.auditLog = auditLog;
    }

    @Override
    public List<JdbcForeignKey> apply(String schemaName, String tableName) {
        String methodName = "getJdbcImportedKeys";
        try {
            return Optional.ofNullable(jdbcMetadata.getImportedKeys(null, schemaName, tableName))
                    .orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Error reading imported keys from JDBC for schema" + schemaName + " and table " + tableName,
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }
}