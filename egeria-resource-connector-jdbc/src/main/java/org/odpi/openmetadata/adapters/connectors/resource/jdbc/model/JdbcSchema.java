/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.resource.jdbc.model;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Represents a schema as returned by the JDBC api. Fields are the ones described in {@link DatabaseMetaData}
 */
public class JdbcSchema {

    private final String tableSchem;
    private final String tableCatalog;

    private final ResultSetMetaData resultSetMetaData;

    public JdbcSchema(String tableSchem, String tableCatalog, ResultSetMetaData resultSetMetaData){
        this.tableSchem = tableSchem;
        this.tableCatalog = tableCatalog;
        this.resultSetMetaData = resultSetMetaData;
    }

    public String getTableSchem() {
        return tableSchem;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public ResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    public static JdbcSchema create(ResultSet resultSet) throws SQLException {
        String tableSchem = resultSet.getString("TABLE_SCHEM");
        String tableCat = resultSet.getString("TABLE_CATALOG");

        return new JdbcSchema(tableSchem, tableCat, resultSet.getMetaData());
    }

}
