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

    private JdbcCatalog(String tableCat){
        this.tableCat = tableCat;
    }

    public String getTableCat() {
        return tableCat;
    }


    public static JdbcCatalog create(ResultSet resultSet) throws SQLException {
        String tableCat = resultSet.getString("TABLE_CAT");

        return new JdbcCatalog(tableCat);
    }

}
