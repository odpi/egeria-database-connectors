/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;


import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.DatabaseProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/*
This class holds the the attributes of a postgres database
It's not a true mapping to the database entity and includes data needed
to build the egeria entity ( Fields such as Version have been added
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

        if (getName() != null)
            props.put("name", getName());

        if (getEncoding() != null)
            props.put("encoding", getEncoding());

        if (getCollate() != null)
            props.put("collate", getCollate());

        if (getCtype() != null)
            props.put("ctype", getCtype());

        if (getVersion() != null)
            props.put("version", getVersion());

        return props;
    }

    public String getQualifiedName()
    {
        return getName();
    }

    public boolean equals(DatabaseElement element)
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
