/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.resource.jdbc.model;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Represents a catalog as returned by the JDBC api. Fields are the ones described in {@link DatabaseMetaData}
 */
public class JdbcCatalog {

    private final String tableCat;

    private final ResultSetMetaData resultSetMetaData;

    public JdbcCatalog(String tableCat, ResultSetMetaData resultSetMetaData){
        this.tableCat = tableCat;
        this.resultSetMetaData = resultSetMetaData;
    }

    public String getTableCat() {
        return tableCat;
    }

    public ResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    public static JdbcCatalog create(ResultSet resultSet) throws SQLException {
        String tableCat = resultSet.getString("TABLE_CAT");

        return new JdbcCatalog(tableCat, resultSet.getMetaData());
    }

}
