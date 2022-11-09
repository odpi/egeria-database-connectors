/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc;

import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.JdbcMetadata;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.JdbcMetadataTransfer;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.JdbcConnector;
import org.odpi.openmetadata.frameworks.connectors.Connector;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXCEPTION_ON_CONTEXT_RETRIEVAL;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXCEPTION_READING_JDBC;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_COMPLETE;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_CONNECTION_FAIL;
import static org.odpi.openmetadata.adapters.connectors.integration.jdbc.ffdc.JdbcConnectorAuditCode.EXITING_ON_INTEGRATION_CONTEXT_FAIL;

public class JdbcIntegrationConnector extends DatabaseIntegratorConnector{

    private JdbcConnector jdbcConnector;

    @Override
    public void initializeEmbeddedConnectors(List<Connector> embeddedConnectors) {
        super.initializeEmbeddedConnectors(embeddedConnectors);
        jdbcConnector = (JdbcConnector) embeddedConnectors.get(0);
    }

    @Override
    public void refresh() {
        String methodName = "JdbcIntegrationConnector.refresh";
        String exitAction = "Exiting " + methodName;

        Connection connection = connect();
        if(connection == null){
            auditLog.logMessage(exitAction, EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName));
            return;
        }
        DatabaseMetaData databaseMetaData = getDatabaseMetadata(connection);
        if(databaseMetaData == null){
            auditLog.logMessage(exitAction, EXITING_ON_CONNECTION_FAIL.getMessageDefinition(methodName));
            close(connection);
            return;
        }

        JdbcMetadataTransfer jdbcMetadataTransfer = createJdbcMetadataTransfer(databaseMetaData);
        if(jdbcMetadataTransfer == null){
            auditLog.logMessage(exitAction, EXITING_ON_INTEGRATION_CONTEXT_FAIL.getMessageDefinition(methodName));
            close(connection);
            return;
        }

        jdbcMetadataTransfer.execute();
        auditLog.logMessage(exitAction, EXITING_ON_COMPLETE.getMessageDefinition(methodName));
        close(connection);
    }

    private Connection connect(){
        String methodName = "connect";
        try {
            return jdbcConnector.asDataSource().getConnection();
        } catch (SQLException sqlException) {
            auditLog.logException("Connecting to target database server",
                    EXCEPTION_READING_JDBC.getMessageDefinition(methodName), sqlException);
        }
        return null;
    }

    public void close(Connection connection) {
        String methodName = "close";
        try{
            if(!connection.isClosed()){
                connection.close();
            }
        } catch (SQLException sqlException) {
            auditLog.logException("Closing connection to database server",
                    EXCEPTION_READING_JDBC.getMessageDefinition(methodName), sqlException);
        }
    }

    private DatabaseMetaData getDatabaseMetadata(Connection connection){
        String methodName = "getDatabaseMetadata";
        try{
            return connection.getMetaData();
        }catch (SQLException sqlException){
            auditLog.logException("Extracting database metadata",
                    EXCEPTION_READING_JDBC.getMessageDefinition(methodName), sqlException);
        }
        return null;
    }

    private JdbcMetadataTransfer createJdbcMetadataTransfer(DatabaseMetaData databaseMetaData){
        String methodName = "createJdbcMetadataTransfer";
        try{
            String connectorTypeQualifiedName = jdbcConnector.getConnection().getConnectorType().getConnectorProviderClassName();
            return new JdbcMetadataTransfer(new JdbcMetadata(databaseMetaData), this.getContext(),
                    connectorTypeQualifiedName, auditLog);
        }catch (ConnectorCheckedException e) {
            auditLog.logException("Extracting integration context",
                    EXCEPTION_ON_CONTEXT_RETRIEVAL.getMessageDefinition(methodName), e);
        }
        return null;
    }

}
