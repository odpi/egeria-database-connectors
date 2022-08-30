/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseColumnProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabasePrimaryKeyProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcColumn;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcPrimaryKey;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.sql.JDBCType;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ColumnTransfer implements Function<JdbcColumn, DatabaseColumnElement> {

    private final Omas omas;
    private final AuditLog auditLog;
    private final List<DatabaseColumnElement> omasColumns;
    private final List<JdbcPrimaryKey> jdbcPrimaryKeys;
    private final DatabaseTableElement omasTable;

    public ColumnTransfer(Omas omas, AuditLog auditLog, List<DatabaseColumnElement> omasColumns,
                          List<JdbcPrimaryKey> jdbcPrimaryKeys, DatabaseTableElement omasTable) {
        this.omas = omas;
        this.auditLog = auditLog;
        this.omasColumns = omasColumns;
        this.jdbcPrimaryKeys = jdbcPrimaryKeys;
        this.omasTable = omasTable;
    }

    @Override
    public DatabaseColumnElement apply(JdbcColumn jdbcColumn) {
        DatabaseColumnProperties columnProperties = buildColumnProperties(jdbcColumn, omasTable);

        Optional<DatabaseColumnElement> omasColumn = omasColumns.stream()
                .filter(dce -> dce.getDatabaseColumnProperties().getQualifiedName().equals(columnProperties.getQualifiedName()))
                .findFirst();

        if(omasColumn.isPresent()){
            omas.updateColumn(omasColumn.get(), columnProperties);
            this.setPrimaryKey(jdbcPrimaryKeys, jdbcColumn, omasColumn.get().getElementHeader().getGUID());
            return omasColumn.get();
        }
        Optional<String> columnGuid = omas.createColumn(omasTable.getElementHeader().getGUID(), columnProperties);
        columnGuid.ifPresent(s -> this.setPrimaryKey(jdbcPrimaryKeys, jdbcColumn, s));

        return null;
    }

    private DatabaseColumnProperties buildColumnProperties(JdbcColumn jdbcColumn, DatabaseTableElement omasTable){
        DatabaseColumnProperties properties = new DatabaseColumnProperties();
        properties.setDisplayName(jdbcColumn.getColumnName());
        properties.setQualifiedName(omasTable.getDatabaseTableProperties().getQualifiedName() + "::" + jdbcColumn.getColumnName());
        properties.setDataType(extractDataType(jdbcColumn.getDataType()));

        return properties;
    }

    private String extractDataType(int jdbcDataType){
        String dataType = "<unknown>";
        try {
            dataType = JDBCType.valueOf(jdbcDataType).getName();
        }catch(IllegalArgumentException iae){
            // do nothing
        }
        return dataType;
    }

    private void setPrimaryKey(List<JdbcPrimaryKey> jdbcPrimaryKeys, JdbcColumn jdbcColumn, String columnGuid){
        Optional<JdbcPrimaryKey> jdbcPrimaryKey = jdbcPrimaryKeys.stream().filter(
                key -> key.getTableSchem().equals(jdbcColumn.getTableSchem())
                        && key.getTableName().equals(jdbcColumn.getTableName())
                        && key.getColumnName().equals(jdbcColumn.getColumnName())
        ).findFirst();
        if(jdbcPrimaryKey.isEmpty()){
            return;
        }

        DatabasePrimaryKeyProperties primaryKeyProperties = buildPrimaryKeyProperties(jdbcPrimaryKey.get());
        omas.setPrimaryKey(columnGuid, primaryKeyProperties);
    }

    private DatabasePrimaryKeyProperties buildPrimaryKeyProperties(JdbcPrimaryKey jdbcPrimaryKey){
        DatabasePrimaryKeyProperties properties = new DatabasePrimaryKeyProperties();
        properties.setName(jdbcPrimaryKey.getPkName());

        return properties;
    }



}