/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.resource.jdbc;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcCatalog;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcTable;
import org.odpi.openmetadata.frameworks.connectors.ConnectorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JdbcConnector works exclusively with JDBC API to retrieve metadata. It internally manages the connection but specific
 * calls to open and close the connection are needed, and in some cases converts the result into specific objects.
 *
 * Generic use case is:
 * <code>
 * connector.open()
 * // connector usage
 * connector.getSchemas()
 * // more connector usage
 * connector.close()
 * </code>
 */
public class JdbcConnector extends ConnectorBase implements JdbcMetadata {

    private static final Logger log = LoggerFactory.getLogger(JdbcConnector.class);

    private DatabaseMetaData databaseMetaData;

    @Override
    public boolean open(){
        String url = (String)connectionProperties.getConfigurationProperties().get("url");
        String user = connectionProperties.getUserId();
        String password = connectionProperties.getClearPassword();

        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            databaseMetaData = connection.getMetaData();
            return true;
        } catch (SQLException sqlException){
            log.error("Error when creating connection to database server", sqlException);
        }
        return false;
    }

    @Override
    public void close() {
        try{
            Connection connection = databaseMetaData.getConnection();
            if(!connection.isClosed()){
                connection.close();
            }
        } catch (SQLException sqlException) {
            log.error("Error when closing connection to database server", sqlException);
        }
        databaseMetaData = null;
    }

    @Override
    public String getUserName() throws SQLException {
        return this.databaseMetaData.getUserName();
    }

    @Override
    public String getDriverName() throws SQLException {
        return this.databaseMetaData.getDriverName();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return this.databaseMetaData.getDatabaseProductName();
    }

    @Override
    public String getUrl() throws SQLException {
        return this.databaseMetaData.getURL();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return this.databaseMetaData.getDatabaseProductVersion();
    }

    @Override
    public List<String> getTableTypes() throws SQLException {
        List<String> result = new ArrayList<>();

        ResultSet tableTypes = this.databaseMetaData.getTableTypes();
        while(tableTypes.next()){
            result.add(tableTypes.getString("TABLE_TYPE"));
        }
        close(tableTypes);

        return result;
    }

    @Override
    public List<JdbcColumn> getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        List<JdbcColumn> result = new ArrayList<>();

        ResultSet columns = this.databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        while(columns.next()){
            result.add(JdbcColumn.create(columns));
        }
        close(columns);

        return result;
    }

    @Override
    public List<JdbcTable> getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        List<JdbcTable> result = new ArrayList<>();

        ResultSet tables = this.databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);
        while(tables.next()){
            result.add(JdbcTable.create(tables));
        }
        close(tables);

        return result;
    }

    @Override
    public List<JdbcSchema> getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<JdbcSchema> result = new ArrayList<>();

        ResultSet schemas = this.databaseMetaData.getSchemas(catalog, schemaPattern);
        while(schemas.next()){
            result.add(JdbcSchema.create(schemas));
        }
        close(schemas);

        return result;
    }

    @Override
    public List<JdbcSchema> getSchemas() throws SQLException {
        List<JdbcSchema> result = new ArrayList<>();

        ResultSet schemas = this.databaseMetaData.getSchemas();
        while(schemas.next()){
            result.add(JdbcSchema.create(schemas));
        }
        close(schemas);

        return result;
    }

    @Override
    public List<JdbcCatalog> getCatalogs() throws SQLException {
        List<JdbcCatalog> result = new ArrayList<>();

        ResultSet catalogs = this.databaseMetaData.getCatalogs();
        while(catalogs.next()){
            result.add(JdbcCatalog.create(catalogs));
        }
        close(catalogs);

        return result;
    }

    private void close(ResultSet resultSet) throws SQLException {
        if(resultSet.isClosed()){
            return;
        }
        resultSet.close();
    }

}
