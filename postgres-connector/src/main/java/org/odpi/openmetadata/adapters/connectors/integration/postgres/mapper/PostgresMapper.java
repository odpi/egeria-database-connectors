/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.postgres.mapper;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseSchemaProperties;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresSchema;

public class PostgresMapper
{

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db     the postgres database attributes to be
     * @return       the egeria datbase propertys
     */

    public DatabaseProperties mapDatabaseProperties(PostgresDatabase db )
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
    public DatabaseSchemaProperties mapSchemaProlperties(PostgresSchema sch)
    {
        DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
        schemaProps.setDisplayName(sch.getQualifiedName());
        schemaProps.setQualifiedName(sch.getQualifiedName());
        schemaProps.setOwner(sch.getSchema_owner());
        schemaProps.setAdditionalProperties(sch.getProperties());

        return schemaProps;
    }
}
