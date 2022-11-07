/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Transfers metadata of a schema
 */
public class SchemaTransfer implements Function<JdbcSchema, DatabaseSchemaElement> {

    private final Omas omas;
    private final AuditLog auditLog;
    private final List<DatabaseSchemaElement> omasSchemas;
    private final String databaseQualifiedName;
    private final String databaseGuid;

    public SchemaTransfer(Omas omas, AuditLog auditLog, List<DatabaseSchemaElement> omasSchemas, String databaseQualifiedName, String databaseGuid) {
        this.omas = omas;
        this.auditLog = auditLog;
        this.omasSchemas = omasSchemas;
        this.databaseQualifiedName = databaseQualifiedName;
        this.databaseGuid = databaseGuid;
    }

    /**
     * Triggers schema metadata transfer
     *
     * @param jdbcSchema schema
     *
     * @return schema element
     */
    @Override
    public DatabaseSchemaElement apply(JdbcSchema jdbcSchema) {
        DatabaseSchemaProperties schemaProperties = buildSchemaProperties(jdbcSchema);

        Optional<DatabaseSchemaElement> omasSchema = omasSchemas.stream()
                .filter(dse -> dse.getDatabaseSchemaProperties()
                        .getQualifiedName().equals(schemaProperties.getQualifiedName()))
                .findFirst();

        if (omasSchema.isPresent()) {
            omas.updateSchema(omasSchema.get().getElementHeader().getGUID(), schemaProperties);
            auditLog.logMessage("Updated schema with qualified name " + schemaProperties.getQualifiedName(),
                    null);
            return omasSchema.get();
        }

        omas.createSchema(databaseGuid, schemaProperties);
        auditLog.logMessage("Created schema with qualified name " + schemaProperties.getQualifiedName(),
                null);
        return null;
    }

    /**
     * Build schema properties
     *
     * @param jdbcSchema schema
     *
     * @return properties
     */
    private DatabaseSchemaProperties buildSchemaProperties(JdbcSchema jdbcSchema) {
        DatabaseSchemaProperties jdbcSchemaProperties = new DatabaseSchemaProperties();
        jdbcSchemaProperties.setDisplayName(jdbcSchema.getTableSchema());
        jdbcSchemaProperties.setQualifiedName(databaseQualifiedName + "::" + jdbcSchema.getTableSchema());
        return jdbcSchemaProperties;
    }

}
