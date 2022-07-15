/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorContext;

import java.util.function.Consumer;

public class RemoveDatabaseTableConsumer implements Consumer<DatabaseTableElement> {

    DatabaseIntegratorContext databaseIntegratorContext;

    RemoveDatabaseTableConsumer(DatabaseIntegratorContext databaseIntegratorContext){
        this.databaseIntegratorContext = databaseIntegratorContext;
    }

    @Override
    public void accept(DatabaseTableElement databaseTableElement) {
        try {
            databaseIntegratorContext.removeDatabaseSchema(databaseTableElement.getElementHeader().getGUID(),
                    databaseTableElement.getDatabaseTableProperties().getQualifiedName());
        } catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e) {
            e.printStackTrace();
        }
    }

}
