/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.function.Consumer;

public class RemoveDatabaseColumnConsumer implements Consumer<DatabaseColumnElement> {

    DatabaseIntegratorContext databaseIntegratorContext;

    RemoveDatabaseColumnConsumer(DatabaseIntegratorContext databaseIntegratorContext){
        this.databaseIntegratorContext = databaseIntegratorContext;
    }

    @Override
    public void accept(DatabaseColumnElement databaseColumnElement) {
        try {
            databaseIntegratorContext.removeDatabaseSchema(databaseColumnElement.getElementHeader().getGUID(),
                    databaseColumnElement.getDatabaseColumnProperties().getQualifiedName());
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            e.printStackTrace();
        }
    }

}
