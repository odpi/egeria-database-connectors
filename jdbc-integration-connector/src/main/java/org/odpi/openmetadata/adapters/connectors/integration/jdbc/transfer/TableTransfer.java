/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseTableProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.model.JdbcTable;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.TRANSFER_COMPLETE_FOR_DB_OBJECT;

/**
 * Transfers metadata of a table
 */
public class TableTransfer implements Function<JdbcTable, DatabaseTableElement> {

    private final Omas omas;
    private final AuditLog auditLog;
    private final List<DatabaseTableElement> omasTables;
    private final String schemaQualifiedName;
    private final String schemaGuid;

    public TableTransfer(Omas omas, AuditLog auditLog, List<DatabaseTableElement> omasTables, String schemaQualifiedName, String schemaGuid) {
        this.omas = omas;
        this.auditLog = auditLog;
        this.omasTables = omasTables;
        this.schemaQualifiedName = schemaQualifiedName;
        this.schemaGuid = schemaGuid;
    }

    /**
     * Triggers table metadata transfer
     *
     * @param jdbcTable table
     *
     * @return table element
     */
    @Override
    public DatabaseTableElement apply(JdbcTable jdbcTable) {
        DatabaseTableProperties tableProperties = this.buildTableProperties(jdbcTable);

        Optional<DatabaseTableElement> omasTable = omasTables.stream()
                .filter(dte -> dte.getDatabaseTableProperties().getQualifiedName().equals(tableProperties.getQualifiedName()))
                .findFirst();

        if(omasTable.isPresent()){
            omas.updateTable(omasTable.get().getElementHeader().getGUID(), tableProperties);
            auditLog.logMessage("Updated table with qualified name " + tableProperties.getQualifiedName(),
                    TRANSFER_COMPLETE_FOR_DB_OBJECT.getMessageDefinition("table " + tableProperties.getQualifiedName()));
            return omasTable.get();
        }

        omas.createTable(schemaGuid, tableProperties);
        auditLog.logMessage("Created table with qualified name " + tableProperties.getQualifiedName(),
                TRANSFER_COMPLETE_FOR_DB_OBJECT.getMessageDefinition("table " + tableProperties.getQualifiedName()));
        return null;
    }

    /**
     * Build table properties
     *
     * @param jdbcTable table
     *
     * @return properties
     */
    private DatabaseTableProperties buildTableProperties(JdbcTable jdbcTable){
        DatabaseTableProperties jdbcTableProperties = new DatabaseTableProperties();
        jdbcTableProperties.setDisplayName(jdbcTable.getTableName());
        jdbcTableProperties.setQualifiedName(schemaQualifiedName + "::" + jdbcTable.getTableName());

        return jdbcTableProperties;
    }

}
