/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_READING_JDBC;

/**
 * Manages the getSchemas call to jdbc
 */
class JdbcGetSchemas implements Supplier<List<JdbcSchema>> {

    private final JdbcMetadata jdbcMetadata;
    private final AuditLog auditLog;

    JdbcGetSchemas(JdbcMetadata jdbcMetadata, AuditLog auditLog) {
        this.jdbcMetadata = jdbcMetadata;
        this.auditLog = auditLog;
    }

    /**
     * Get all schemas
     *
     * @return schemas
     */
    @Override
    public List<JdbcSchema> get(){
        String methodName = "JdbcGetSchemas";
        try {
            return Optional.ofNullable(jdbcMetadata.getSchemas()).orElseGet(ArrayList::new);
        } catch (SQLException sqlException) {
            auditLog.logException("Reading schemas from JDBC",
                    ERROR_READING_JDBC.getMessageDefinition(methodName, sqlException.getMessage()), sqlException);
        }
        return new ArrayList<>();
    }

}