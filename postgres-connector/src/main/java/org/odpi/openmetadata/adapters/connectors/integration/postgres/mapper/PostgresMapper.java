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
     * for a schema from Postgres and adds the data to Egeria
     *
     * @param db     the Postgres database attributes to be
     * @return       the Egeria database properties
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
     * for a schema from Postgres and adds the data to Egeria
     *
     * @param sch     the Postgres database attributes to be
     * @return        the Egeria schema properties
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
     * for a schema from Postgres and adds the data to Egeria
     *
     * @param table     the Postgres table properties
     * @return          the Egeria table properties
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
     * mapping function that reads converts a Postgres table properties to an Egeria DatabaseViewProperties
     * for a schema from Postgres and adds the data to Egeria
     *
     * @param table     the Postgres table properties
     * @return          the Egeria view properties
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
     * for a schema from Postgres and adds the data to Egeria
     *
     * @param col    the Postgres column properties
     * @return          the Egeria column properties
     */
    public static DatabaseColumnProperties getColumnProperties(PostgresColumn col)
    {
        DatabaseColumnProperties colProps = new DatabaseColumnProperties();
        colProps.setDisplayName(col.getTable_name());
        colProps.setQualifiedName(col.getQualifiedName());

        colProps.setDataType(col.getData_type());

        if( col.getMaximum_cardinality() != null )
        {
            try
            {
                colProps.setMaxCardinality(Integer.valueOf(col.getMaximum_cardinality()));
            }
            catch (NumberFormatException error)
            {
                //if we can't make an Integer out of it
                //just leave the property unset

            }
        }

        colProps.setAdditionalProperties(col.getProperties());

        return colProps;
    }

}
