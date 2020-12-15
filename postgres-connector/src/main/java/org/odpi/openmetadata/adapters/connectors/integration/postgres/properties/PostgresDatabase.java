/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;


import java.util.HashMap;
import java.util.Map;

/*
This class holds the the attributes of a postgres database
It's not a true mapping to the database entity and includes data needed
to build the egeria entity ( Fields such as Version have been added
The class is immutable
 */
public class PostgresDatabase
{
    private String Name;
    private String Owner;
    private String Encoding;
    private String Collate;
    private String Ctype;
    private String AccessPrivileges;
    private String Version;
    private String Instance;

    public PostgresDatabase(String name, String owner, String encoding, String collate, String ctype, String accessPrivileges, String version, String instance)
    {
        Name = name;
        Owner = owner;
        Encoding = encoding;
        Collate = collate;
        Ctype = ctype;
        AccessPrivileges = accessPrivileges;
        Version = version;
        Instance = instance;
    }

    public String getName() {
        return Name;
    }
    public String getOwner() {
        return Owner;
    }
    public String getEncoding() {
        return Encoding;
    }
    public String getCollate() {
        return Collate;
    }
    public String getCtype() {
        return Ctype;
    }
    public String getAccessPrivileges() {
        return AccessPrivileges;
    }
    public String getVersion() { return Version; }
    public String getInstance() { return Instance; }

    public Map< String, String> getProperties ()
    {
        Map<String, String> props = new HashMap<>();

        if( getName() != null )
            props.put("name", getName());

        if( getOwner() != null )
            props.put("owner", getOwner() );

        if( getEncoding() != null )
            props.put("encoding", getEncoding() );

        if( getCollate() != null )
            props.put("collate", getCollate() );

        if( getCtype() != null )
            props.put("ctype", getCtype());

        if( getAccessPrivileges() != null )
            props.put("accessPrivileges", getAccessPrivileges() );

        if( getVersion() != null )
            props.put("version", getVersion() );

        if( getInstance() != null )
            props.put("instance", getInstance() );

        return props;
    }

    public String getQualifiedName ( ) {
        return getInstance() + "::" + getOwner() + "::" + getName();
    }
}
