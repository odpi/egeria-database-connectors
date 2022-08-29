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
public class JdbcPrimaryKey {

    private final String tableCat;
    private final String tableSchem;
    private final String tableName;
    private final String columnName;
    private final String keySeq;
    private final String pkName;

    private JdbcPrimaryKey(String tableCat, String tableSchem, String tableName, String columnName, String keySeq,
                          String pkName){
        this.tableCat = tableCat;
        this.tableSchem = tableSchem;
        this.tableName = tableName;
        this.columnName = columnName;
        this.keySeq = keySeq;
        this.pkName = pkName;
    }

    public String getTableCat() {
        return tableCat;
    }

    public String getTableSchem() {
        return tableSchem;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getKeySeq() {
        return keySeq;
    }

    public String getPkName() {
        return pkName;
    }

    public static JdbcPrimaryKey create(ResultSet resultSet) throws SQLException {
        String tableCat = resultSet.getString("TABLE_CAT");
        String tableSchem = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        String columnName = resultSet.getString("COLUMN_NAME");
        String keySeq = resultSet.getString("KEY_SEQ");
        String pkName = resultSet.getString("PK_NAME");

        return new JdbcPrimaryKey(tableCat, tableSchem, tableName, columnName, keySeq, pkName);
    }

}
