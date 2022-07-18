/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.resource.jdbc;

import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcCatalog;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcSchema;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcTable;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

public interface JdbcMetadata {

    /**
     * Opens a connection to designated server
     */
    boolean open();

    /**
     * Closes the connection to designated server
     */
    void close();

    /**
     * @see DatabaseMetaData#getUserName()
     */
    String getUserName() throws SQLException;

    /**
     * @see DatabaseMetaData#getDriverName()
     */
    String getDriverName() throws SQLException;

    /**
     * @see java.sql.DatabaseMetaData#getDatabaseProductName
     */
    String getDatabaseProductName() throws SQLException;

    /**
     * @see DatabaseMetaData#getURL()
     */
    String getUrl() throws SQLException;

    /**
     * @see DatabaseMetaData#getDatabaseProductVersion()
     */
    String getDatabaseProductVersion() throws SQLException;

    /**
     * @see DatabaseMetaData#getTableTypes()
     */
    List<String> getTableTypes() throws SQLException;

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
     * @see DatabaseMetaData#getColumns(String, String, String, String)
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
     * @see DatabaseMetaData#getTables(String, String, String, String[])
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
     * @see DatabaseMetaData#getSchemas(String, String)
     */
    List<JdbcSchema> getSchemas(String catalog, String schemaPattern) throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcSchema}
     *
     * @return jdbc schemas
     *
     * @throws  SQLException sql exception
     *
     * @see DatabaseMetaData#getSchemas()
     */
    List<JdbcSchema> getSchemas() throws SQLException;

    /**
     * Parses the result and converts to {@link JdbcCatalog}
     *
     * @return jdbc catalog
     *
     * @throws  SQLException sql exception
     *
     * @see DatabaseMetaData#getCatalogs()
     */
    List<JdbcCatalog> getCatalogs() throws SQLException;
}
