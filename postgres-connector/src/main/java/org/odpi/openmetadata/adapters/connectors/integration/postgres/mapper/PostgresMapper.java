/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.postgres.mapper;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseColumnProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseTableProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseViewProperties;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresColumn;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresSchema;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresTable;

/**
 * Utility class that provides bean mapping functions
 */
public class PostgresMapper
{

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db     the postgres database attributes to be
     * @return       the egeria datbase propertys
     */
    public static DatabaseProperties getDatabaseProperties(PostgresDatabase db )
    {

            DatabaseProperties dbProps = new DatabaseProperties();
            dbProps.setDisplayName(db.getQualifiedName());
            dbProps.setQualifiedName(db.getQualifiedName());
            dbProps.setDatabaseType("postgres");
            dbProps.setDatabaseVersion(db.getVersion());
            dbProps.setEncodingType(db.getEncoding());
            dbProps.setEncodingLanguage(db.getCtype());
            dbProps.setAdditionalProperties(db.getProperties());

            return dbProps;

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sch     the postgres database attributes to be
     * @return        the egeria schema propertys
     */
    public static DatabaseSchemaProperties getSchemaProperties(PostgresSchema sch)
    {
        DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
        schemaProps.setDisplayName(sch.getQualifiedName());
        schemaProps.setQualifiedName(sch.getQualifiedName());
        schemaProps.setOwner(sch.getSchema_owner());
        schemaProps.setAdditionalProperties(sch.getProperties());

        return schemaProps;
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param table     the postgres table properties
     * @return          the egeria table propertys
     */
    public static DatabaseTableProperties getTableProperties(PostgresTable table)
    {
        DatabaseTableProperties tableProps = new DatabaseTableProperties();
        tableProps.setDisplayName(table.getTable_name());
        tableProps.setQualifiedName(table.getQualifiedName());
        tableProps.setAdditionalProperties( table.getProperties());

        return tableProps;
    }

    /**
     * mapping function that reads converts a postgres table properties to an egeria DatabaseViewProperties
     * for a schema from postgres and adds the data to egeria
     *
     * @param table     the postgres table properties
     * @return          the egeria view properties
     */
    public static DatabaseViewProperties getViewProperties(PostgresTable table)
    {
        DatabaseViewProperties tableProps = new DatabaseViewProperties();
        tableProps.setDisplayName(table.getTable_name());
        tableProps.setQualifiedName(table.getQualifiedName());
        tableProps.setAdditionalProperties( table.getProperties());

        return tableProps;
    }
    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param col    the postgres column properties
     * @return          the egeria column propertys
     */
    public static DatabaseColumnProperties getColumnProperties(PostgresColumn col)
    {
        DatabaseColumnProperties colProps = new DatabaseColumnProperties();
        colProps.setDisplayName(col.getTable_name());
        colProps.setQualifiedName(col.getQualifiedName());
        colProps.setAdditionalProperties(col.getProperties());

        return colProps;
    }

}
