/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;


import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
This class holds the the attributes of a Postgres database
It's not a true mapping to the database entity and includes data needed
to build the Egeria entity ( Fields such as Version have been added
The class is immutable
 */
public class PostgresDatabase
{
    private String Name;
    private String Encoding;
    private String Collate;
    private String Ctype;
    private String Version;

    public PostgresDatabase(String name, String encoding, String collate, String ctype, String version)
    {
        Name = name;
        Encoding = encoding;
        Collate = collate;
        Ctype = ctype;
        Version = version;
    }

    public String getName()
    {
        return Name;
    }

    public String getEncoding()
    {
        return Encoding;
    }

    public String getCollate()
    {
        return Collate;
    }

    public String getCtype()
    {
        return Ctype;
    }

    public String getVersion()
    {
        return Version;
    }

    public Map<String, String> getProperties()
    {
        Map<String, String> props = new HashMap<>();

            props.put("name", getName());
            props.put("encoding", getEncoding());
            props.put("collate", getCollate());
            props.put("ctype", getCtype());
            props.put("version", getVersion());

        return props;
    }

    public String getQualifiedName()
    {
        return getName();
    }

    public boolean isEquivalent(DatabaseElement element)
    {
        boolean result = false;
        Map<String, String> props = element.getDatabaseProperties().getAdditionalProperties();
        if ( props.equals( this.getProperties()))
        {
            result = true;
        }
        return result;
    }

}
