/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseForeignKeyProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.adapters.connectors.resource.jdbc.model.JdbcForeignKey;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;
import java.util.function.Consumer;

public class ForeignKeyTransfer implements Consumer<JdbcForeignKey> {

    private final Omas omas;
    private final AuditLog auditLog;
    private final DatabaseElement database;

    public ForeignKeyTransfer(Omas omas, AuditLog auditLog, DatabaseElement database) {
        this.omas = omas;
        this.auditLog = auditLog;
        this.database = database;
    }

    @Override
    public void accept(JdbcForeignKey jdbcForeignKey) {
        String databaseQualifiedName = database.getDatabaseProperties().getQualifiedName();
        String pkColumnQualifiedName = databaseQualifiedName + "::" + jdbcForeignKey.getPkTableSchem()
                + "::" + jdbcForeignKey.getPkTableName() + "::" + jdbcForeignKey.getPkColumnName();
        String fkColumnQualifiedName = databaseQualifiedName + "::" + jdbcForeignKey.getFkTableSchem()
                + "::" + jdbcForeignKey.getFkTableName() + "::" + jdbcForeignKey.getFkColumnName();

        DatabaseColumnElement pkColumn = determineColumn(omas.findDatabaseColumns(pkColumnQualifiedName));
        DatabaseColumnElement fkColumn = determineColumn(omas.findDatabaseColumns(fkColumnQualifiedName));

        if(pkColumn == null || fkColumn == null){
            return;
        }
        omas.setForeignKey(pkColumn.getElementHeader().getGUID(), fkColumn.getElementHeader().getGUID(),
                buildForeignKeyProperties(jdbcForeignKey));
    }

    private DatabaseColumnElement determineColumn(List<DatabaseColumnElement> columns){
        if(columns.size() == 1){
            return columns.get(0);
        }
        return null;
    }

    private DatabaseForeignKeyProperties buildForeignKeyProperties(JdbcForeignKey jdbcForeignKey){
        DatabaseForeignKeyProperties properties = new DatabaseForeignKeyProperties();
        properties.setName(jdbcForeignKey.getPkName() + " - " + jdbcForeignKey.getFkName());
        properties.setSource(database.getDatabaseProperties().getDatabaseImportedFrom());
        return properties;
    }
}
