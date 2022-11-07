/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.model;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Represents a schema as returned by the JDBC api. Fields are the ones described in {@link DatabaseMetaData}
 */
public class JdbcSchema {

    private final String tableSchema;
    private final String tableCatalog;

    private JdbcSchema(String tableSchema, String tableCatalog){
        this.tableSchema = tableSchema;
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public static JdbcSchema create(ResultSet resultSet) throws SQLException {
        String tableSchema = resultSet.getString("TABLE_SCHEM");
        String tableCat = resultSet.getString("TABLE_CATALOG");

        return new JdbcSchema(tableSchema, tableCat);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if(!(other instanceof JdbcSchema)){
            return false;
        }

        JdbcSchema other_ = (JdbcSchema) other;
        return Objects.equals(getTableCatalog(), other_.getTableCatalog()) &&
                Objects.equals(getTableSchema(), other_.getTableSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableSchema, tableCatalog);
    }
}
