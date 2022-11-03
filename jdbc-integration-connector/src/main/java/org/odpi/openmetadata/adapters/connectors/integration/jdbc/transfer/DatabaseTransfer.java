/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Jdbc;
import org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.requests.Omas;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;

import java.util.List;

/**
 * Creates the database root of the metadata structure the follows
 */
public class DatabaseTransfer {

    private final Jdbc jdbc;
    private final Omas omas;
    private final AuditLog auditLog;

    public DatabaseTransfer(Jdbc jdbc, Omas omas, AuditLog auditLog) {
        this.jdbc = jdbc;
        this.omas = omas;
        this.auditLog = auditLog;
    }

    /**
     * Triggers database metadata transfer
     *
     * @return database element
     */
    public DatabaseElement execute() {
        DatabaseProperties databaseProperties = buildDatabaseProperties();
        String multipleDatabasesFoundMessage = "Querying for a database with qualified name "
                + databaseProperties.getQualifiedName() + " and found multiple. Expecting only one";

        List<DatabaseElement> databasesInOmas = omas.getDatabasesByName(databaseProperties.getQualifiedName());
        if (databasesInOmas.isEmpty()) {
            omas.createDatabase(databaseProperties);
        } else {
            if(databasesInOmas.size() > 1){
                auditLog.logMessage(multipleDatabasesFoundMessage, null);
                return null;
            }
            omas.updateDatabase(databasesInOmas.get(0).getElementHeader().getGUID(), databaseProperties);
        }

        databasesInOmas = omas.getDatabasesByName(databaseProperties.getQualifiedName());
        if(databasesInOmas.size() == 1){
            auditLog.logMessage("Database transferred with qualified name " + databaseProperties.getQualifiedName(),
                    null);
            return databasesInOmas.get(0);
        }
        auditLog.logMessage(multipleDatabasesFoundMessage, null);
        return null;
    }

    /**
     * Builds database properties
     *
     * @return properties
     */
    private DatabaseProperties buildDatabaseProperties() {
        String driverName = jdbc.getDriverName();
        String databaseProductVersion = jdbc.getDatabaseProductVersion();
        String databaseProductName = jdbc.getDatabaseProductName();
        String url = jdbc.getUrl();
        String urlWithNoParams = url.contains("?") ? url.substring(0, url.indexOf("?")) : url;
        String catalogFromUrl = urlWithNoParams.substring(url.lastIndexOf("/") + 1);

        DatabaseProperties databaseProperties = new DatabaseProperties();
        databaseProperties.setQualifiedName(urlWithNoParams);
        databaseProperties.setDisplayName(catalogFromUrl);
        databaseProperties.setDatabaseInstance(driverName);
        databaseProperties.setDatabaseVersion(databaseProductVersion);
        databaseProperties.setDatabaseType(databaseProductName);
        databaseProperties.setDatabaseImportedFrom(url);

//        Map<String, String> origin = new HashMap<>();
//        Optional<JdbcCatalog> catalog = jdbc.getCatalogs().stream().filter(c -> c.getTableCat().equals(catalogFromUrl)).findFirst();
//        origin.put("jdbcCatalog", catalog.isPresent() ? catalog.get().getTableCat() : "unavailable");
//        databaseProperties.setAdditionalProperties(origin);

        return databaseProperties;
    }

}
