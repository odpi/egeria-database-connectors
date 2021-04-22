/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;

import java.util.HashMap;
import java.util.Map;

public class PostgresSchema {

    final private String catalog_name;
    final private String schema_name;
    final private String schema_owner;
    final private String default_character_set_catalog;
    final private String default_character_set_schema;
    final private String default_character_set_name;
    final private String sql_path;

    public String getCatalog_name() {
        return catalog_name;
    }
    public String getSchema_name() {
        return schema_name;
    }
    public String getSchema_owner() {
        return schema_owner;
    }
    public String getDefault_character_set_catalog() {
        return default_character_set_catalog;
    }
    public String getDefault_character_set_schema() {
        return default_character_set_schema;
    }
    public String getDefault_character_set_name() {
        return default_character_set_name;
    }
    public String getSql_path() {
        return sql_path;
    }


    public PostgresSchema(String catalog_name, String schema_name, String schema_owner, String default_character_set_catalog, String default_character_set_schema, String default_character_set_name, String sql_path)
    {
        this.catalog_name = catalog_name;
        this.schema_name = schema_name;
        this.schema_owner = schema_owner;
        this.default_character_set_catalog = default_character_set_catalog;
        this.default_character_set_schema = default_character_set_schema;
        this.default_character_set_name = default_character_set_name;
        this.sql_path = sql_path;
    }

    public Map<String, String> getProperties()
    {
        Map<String,String> props = new HashMap<>();


        if( getCatalog_name() != null )
            props.put("catalog_name", getCatalog_name());

        if( getSchema_name() != null )
            props.put("schema_name", getSchema_name());

        if( getSchema_owner() != null )
            props.put("schema_owner", getSchema_owner());

        if( getDefault_character_set_catalog() != null )
            props.put("default_character_set_catalog", getDefault_character_set_catalog());

        if( getDefault_character_set_schema() != null )
            props.put("default_character_set_schema", getDefault_character_set_schema());

        if( getDefault_character_set_name() != null )
            props.put("default_character_set_name", getDefault_character_set_name());

        if( getSql_path() != null )
            props.put("sql_path", getSql_path());

        return props;
    }

    public String getQualifiedName ( )
    {
        return getSchema_owner() + "::" + catalog_name + "::" + schema_name;
    }

    public boolean equals(DatabaseSchemaElement element)
    {
        boolean result = false;
        Map<String, String> props = element.getDatabaseSchemaProperties().getAdditionalProperties();
        if ( props.equals( this.getProperties()))
        {
            result = true;
        }
        return result;
    }
}
