/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.resource.jdbc;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcCatalog;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcPrimaryKey;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcTable;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

public interface JdbcMetadata {

    /**
     * Returns the configured connector type guid
     */
    String getConnectorTypeQualifiedName();

    /**
     * Opens a connection to designated server
     */
    boolean open();

    /**
     * Closes the connection to designated server
     */
    void close();

    /**
     * See {@link DatabaseMetaData#getUserName()}
     */
    String getUserName() throws SQLException;

    /**
     * See {@link DatabaseMetaData#getDriverName()}
     */
    String getDriverName() throws SQLException;

    /**
     * See {@link DatabaseMetaData#getDatabaseProductName()}
     */
    String getDatabaseProductName() throws SQLException;

    /**
     * See {@link DatabaseMetaData#getURL()}
     */
    String getUrl() throws SQLException;

    /**
     * See {@link DatabaseMetaData#getDatabaseProductVersion()}
     */
    String getDatabaseProductVersion() throws SQLException;

    /**
     * See {@link DatabaseMetaData#getTableTypes()}
     */
    List<String> getTableTypes() throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcPrimaryKey}
     *
     * @param catalog catalog
     * @param schema schema
     * @param table table
     *
     * @return jdbc primary keys
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getPrimaryKeys(String, String, String)}
     */
    List<JdbcPrimaryKey> getPrimaryKeys(String catalog, String schema, String table) throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcColumn}
     *
     * @param catalog catalog
     * @param schemaPattern schema
     * @param tableNamePattern table
     * @param columnNamePattern column
     *
     * @return jdbc columns
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getColumns(String, String, String, String)}
     */
    List<JdbcColumn> getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcTable}
     *
     * @param catalog catalog
     * @param schemaPattern schema
     * @param tableNamePattern table
     * @param types types
     *
     * @return jdbc tables
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getTables(String, String, String, String[])}
     */
    List<JdbcTable> getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcSchema}
     *
     * @param catalog catalog
     * @param schemaPattern schema
     *
     * @return jdbc schemas
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getSchemas(String, String)}
     */
    List<JdbcSchema> getSchemas(String catalog, String schemaPattern) throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcSchema}
     *
     * @return jdbc schemas
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getSchemas()}
     */
    List<JdbcSchema> getSchemas() throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcCatalog}
     *
     * @return jdbc catalog
     *
     * @throws  SQLException sql exception
     *
     * See {@link DatabaseMetaData#getCatalogs()}
     */
    List<JdbcCatalog> getCatalogs() throws SQLException;
}
