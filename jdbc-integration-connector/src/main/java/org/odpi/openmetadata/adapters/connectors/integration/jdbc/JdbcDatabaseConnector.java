/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc;

import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.JdbcMetadataTransfer;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcMetadata;
import org.odpi.openmetadata.frameworks.connectors.Connector;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import java.util.List;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_TRANSFER_FAIL;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_COMPLETE;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_CONNECTION_FAIL;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_INTEGRATION_CONTEXT_FAIL;

public class JdbcDatabaseConnector extends DatabaseIntegratorConnector{

    private JdbcMetadata jdbcMetadataConnector;

    @Override
    public void initializeEmbeddedConnectors(List<Connector> embeddedConnectors) {
        super.initializeEmbeddedConnectors(embeddedConnectors);
        jdbcMetadataConnector = (JdbcMetadata) embeddedConnectors.get(0);
    }

    @Override
    public void refresh() {
        String methodName = "refresh";
        String exitAction = "Exiting " + methodName;

        boolean successfulConnection = jdbcMetadataConnector.open();
        if(!successfulConnection){
            auditLog.logMessage(exitAction, EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName));
            return;
        }
        JdbcMetadataTransfer jdbcMetadataTransfer = createJdbcMetadataTransfer();
        if(jdbcMetadataTransfer == null){
            auditLog.logMessage(exitAction, EXITING_ON_INTEGRATION_CONTEXT_FAIL.getMessageDefinition(methodName));
            return;
        }

        boolean successfulTransfer = jdbcMetadataTransfer.execute();

        if(successfulTransfer) {
            auditLog.logMessage(exitAction, EXITING_ON_COMPLETE.getMessageDefinition(methodName));
        }else{
            auditLog.logMessage(exitAction, EXITING_ON_TRANSFER_FAIL.getMessageDefinition(methodName));
        }
        jdbcMetadataConnector.close();
    }

    private JdbcMetadataTransfer createJdbcMetadataTransfer(){
        String methodName = "createJdbcMetadataTransfer";
        try{
            return new JdbcMetadataTransfer(this.jdbcMetadataConnector, this.getContext(), auditLog);
        }catch (ConnectorCheckedException e) {
            auditLog.logException("Extracting integration context",
                    EXITING_ON_INTEGRATION_CONTEXT_FAIL.getMessageDefinition(methodName), e);
        }
        return null;
    }

}
