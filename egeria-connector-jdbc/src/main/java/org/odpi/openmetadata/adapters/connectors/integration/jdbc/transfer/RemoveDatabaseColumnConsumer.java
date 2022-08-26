/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.function.Consumer;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.ERROR_WHEN_REMOVING_ELEMENT_IN_OMAS;

class RemoveDatabaseColumnConsumer implements Consumer<DatabaseColumnElement> {

    private final DatabaseIntegratorContext databaseIntegratorContext;
    private final AuditLog auditLog;

    RemoveDatabaseColumnConsumer(DatabaseIntegratorContext databaseIntegratorContext, AuditLog auditLog){
        this.databaseIntegratorContext = databaseIntegratorContext;
        this.auditLog = auditLog;
    }

    @Override
    public void accept(DatabaseColumnElement databaseColumnElement) {
        String columnGuid = databaseColumnElement.getElementHeader().getGUID();
        String columnQualifiedName = databaseColumnElement.getDatabaseColumnProperties().getQualifiedName();
        try {
            databaseIntegratorContext.removeDatabaseColumn(columnGuid, columnQualifiedName);
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            auditLog.logMessage("Removing column from omas",
                    ERROR_WHEN_REMOVING_ELEMENT_IN_OMAS.getMessageDefinition(columnGuid, columnQualifiedName));
        }
    }

}
