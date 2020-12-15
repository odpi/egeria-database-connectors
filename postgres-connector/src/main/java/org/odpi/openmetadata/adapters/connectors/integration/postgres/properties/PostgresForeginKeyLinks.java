/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;

import java.util.HashMap;
import java.util.Map;

public class PostgresForeginKeyLinks {

    private final String table_schema;
    private final String constraint_name;
    private final String table_name;
    private final String column_name;
    private final String foreign_table_schema;
    private final String foreign_table_name;
    private final String foreign_column_name;

    public PostgresForeginKeyLinks(String table_schema, String constraint_name, String table_name, String column_name, String foreign_table_schema, String foreign_table_name, String foreign_column_name)
    {
        this.table_schema = table_schema;
        this.constraint_name = constraint_name;
        this.table_name = table_name;
        this.column_name = column_name;
        this.foreign_table_schema = foreign_table_schema;
        this.foreign_table_name = foreign_table_name;
        this.foreign_column_name = foreign_column_name;
    }

    public String getImportedColumnQualifiedName()
    {
        return table_schema +"." +
                table_name + "." +
                column_name ;
    }

    public String getExportedColumnQualifiedName()
    {
        return foreign_table_schema+"." +
                foreign_table_name + "." +
                foreign_column_name;
    }

    public Map< String, String> getProperties ()
    {
        Map<String, String> props = new HashMap<>();

        if( table_schema != null )
            props.put("name", table_schema );

        if( constraint_name != null )
            props.put("owner", constraint_name );

        if( table_name != null )
            props.put("encoding", table_name );

        if( column_name != null )
            props.put("collate", column_name );

        if( foreign_table_name != null )
            props.put("ctype", foreign_table_name );

        if( foreign_table_schema != null )
            props.put("accessPrivileges", foreign_table_schema );

        if( foreign_column_name != null )
            props.put("version", foreign_column_name );

        return props;
    }

}
