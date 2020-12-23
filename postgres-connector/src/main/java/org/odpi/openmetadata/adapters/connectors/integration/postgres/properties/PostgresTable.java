/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;

import java.util.HashMap;
import java.util.Map;

public class PostgresTable {

    private final String table_catalog;
    private final String table_schema;
    private final String table_name;
    private final String table_type;
    private final String self_referencing_column_name;
    private final String reference_generation;
    private final String user_defined_type_catalog;
    private final String user_defined_type_schema;
    private final String user_defined_type_name;
    private final String is_insertable_into;
    private final String is_typed;
    private final String commit_action;

    public PostgresTable(String table_catalog, String table_schema, String table_name, String table_type, String self_referencing_column_name, String reference_generation, String user_defined_type_catalog, String user_defined_type_schema, String user_defined_type_name, String is_insertable_into, String is_typed, String commit_action) {
        this.table_catalog = table_catalog;
        this.table_schema = table_schema;
        this.table_name = table_name;
        this.table_type = table_type;
        this.self_referencing_column_name = self_referencing_column_name;
        this.reference_generation = reference_generation;
        this.user_defined_type_catalog = user_defined_type_catalog;
        this.user_defined_type_schema = user_defined_type_schema;
        this.user_defined_type_name = user_defined_type_name;
        this.is_insertable_into = is_insertable_into;
        this.is_typed = is_typed;
        this.commit_action = commit_action;
    }
    public String getTable_catalog()
    {
        return table_catalog;
    }

    public String getTable_schema()
    {
        return table_schema;
    }

    public String getTable_name()
    {
        return table_name;
    }

    public String getTable_type()
    {
        return table_type;
    }

    public String getSelf_referencing_column_name()
    {
        return self_referencing_column_name;
    }

    public String getReference_generation()
    {
        return reference_generation;
    }

    public String getUser_defined_type_catalog()
    {
        return user_defined_type_catalog;
    }

    public String getUser_defined_type_schema()
    {
        return user_defined_type_schema;
    }

    public String getUser_defined_type_name()
    {
        return user_defined_type_name;
    }

    public String getIs_insertable_into()
    {
        return is_insertable_into;
    }

    public String getIs_typed()
    {
        return is_typed;
    }

    public String getCommit_action()
    {
        return commit_action;
    }

    Map<String,String> getProperties()
    {
        Map props = new HashMap();
        props.put("table_catalog", this.table_catalog);
        props.put("table_schema", this.table_schema);
        props.put("table_name", table_name);
        props.put("table_type", table_type);
        props.put("self_referencing_column_name", self_referencing_column_name);
        props.put("reference_generation", reference_generation );
        props.put("user_defined_type_catalog", user_defined_type_catalog );
        props.put("user_defined_type_schema", user_defined_type_schema );
        props.put("user_defined_type_name", user_defined_type_name );
        props.put("is_insertable_into", is_insertable_into );
        props.put( "is_typed", is_typed );
        props.put( "commit_action", commit_action );

        return props;
    }

    public String getQualifiedName ( ) {
        return table_catalog + "." + table_schema + "." + table_type + "." + table_name;
    }
}
