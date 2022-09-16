/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc;

import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.JdbcMetadataTransfer;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcConnector;
import org.odpi.openmetadata.frameworks.connectors.Connector;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.*;

public class JdbcIntegrationConnector extends DatabaseIntegratorConnector{

    private DataSource jdbcConnector;

    @Override
    public void initializeEmbeddedConnectors(List<Connector> embeddedConnectors) {
        super.initializeEmbeddedConnectors(embeddedConnectors);
        jdbcConnector = ((JdbcConnector) embeddedConnectors.get(0)).asDataSource();
    }

    @Override
    public void refresh() {
        String methodName = "refresh";
        String exitAction = "Exiting " + methodName;

        Connection connection = connect();
        if(connection == null){
            auditLog.logMessage(exitAction, EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName));
            return;
        }
        DatabaseMetaData databaseMetaData = getDatabaseMetadata(connection);
        if(databaseMetaData == null){
            auditLog.logMessage(exitAction, EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName));
            disconnect(connection);
            return;
        }

        JdbcMetadataTransfer jdbcMetadataTransfer = createJdbcMetadataTransfer(databaseMetaData);
        if(jdbcMetadataTransfer == null){
            auditLog.logMessage(exitAction, EXITING_ON_INTEGRATION_CONTEXT_FAIL.getMessageDefinition(methodName));
            disconnect(connection);
            return;
        }

        boolean successfulTransfer = jdbcMetadataTransfer.execute();

        if(successfulTransfer) {
            auditLog.logMessage(exitAction, EXITING_ON_COMPLETE.getMessageDefinition(methodName));
        }else{
            auditLog.logMessage(exitAction, EXITING_ON_TRANSFER_FAIL.getMessageDefinition(methodName));
        }
        disconnect(connection);
    }

    private Connection connect(){
        String methodName = "connect";
        try {
            return jdbcConnector.getConnection();
        } catch (SQLException e) {
            auditLog.logException("Connecting to target database server",
                    EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName), e);
        }
        return null;
    }

    public void disconnect(Connection connection) {
        try{
            if(!connection.isClosed()){
                connection.close();
            }
        } catch (SQLException sqlException) {
            auditLog.logMessage("Error when closing connection to database server", null);
        }
    }

    private DatabaseMetaData getDatabaseMetadata(Connection connection){
        try{
            return connection.getMetaData();
        }catch (SQLException sqlException){
            auditLog.logMessage("Extracting database metadata", null);
        }
        return null;
    }

    private JdbcMetadataTransfer createJdbcMetadataTransfer(DatabaseMetaData databaseMetaData){
        String methodName = "createJdbcMetadataTransfer";
        try{
            String connectorTypeQualifiedName =
                    (String) this.connectionProperties.getConfigurationProperties().get("connectorTypeQualifiedName");
            return new JdbcMetadataTransfer(new JdbcMetadata(databaseMetaData), this.getContext(),
                    connectorTypeQualifiedName, auditLog);
        }catch (ConnectorCheckedException e) {
            auditLog.logException("Extracting integration context",
                    EXITING_ON_INTEGRATION_CONTEXT_FAIL.getMessageDefinition(methodName), e);
        }
        return null;
    }

}
