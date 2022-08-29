/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.List;
import java.util.function.Consumer;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_WHEN_REMOVING_ELEMENT_IN_OMAS;

class OmasRemoveTable implements Consumer<DatabaseTableElement> {

    private final DatabaseIntegratorContext databaseIntegratorContext;
    private final AuditLog auditLog;

    OmasRemoveTable(DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog){
        this.databaseIntegratorContext = databaseIntegratorContext;
        this.auditLog = auditLog;
    }

    @Override
    public void accept(DatabaseTableElement databaseTableElement) {
        String tableGuid = databaseTableElement.getElementHeader().getGUID();
        String tableQualifiedName = databaseTableElement.getDatabaseTableProperties().getQualifiedName();
        try {
            List<DatabaseColumnElement> columns = databaseIntegratorContext.getColumnsForDatabaseTable(tableGuid, 0, 0);
            columns.forEach(new OmasRemoveColumn(databaseIntegratorContext, auditLog));

            databaseIntegratorContext.removeDatabaseTable(tableGuid, tableQualifiedName);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logMessage("Removing table from omas",
                    ERROR_WHEN_REMOVING_ELEMENT_IN_OMAS.getMessageDefinition(tableGuid, tableQualifiedName));
        }
    }

}
