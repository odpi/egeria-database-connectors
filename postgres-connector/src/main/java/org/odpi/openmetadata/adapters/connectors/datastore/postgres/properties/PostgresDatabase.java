/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties;


import java.util.HashMap;
import java.util.Map;

public class PostgresDatabase
{
    private String Name;
    private String Owner;
    private String Encoding;
    private String Collate;
    private String Ctype;
    private String AccessPrivileges;
    private String Version;

    public PostgresDatabase(String name, String owner, String encoding, String collate, String ctype, String accessPrivileges, String version)
    {
        Name = name;
        Owner = owner;
        Encoding = encoding;
        Collate = collate;
        Ctype = ctype;
        AccessPrivileges = accessPrivileges;
        Version = version;
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

    public Map< String, String> getProperties ()
    {
        Map<String, String> props = new HashMap<>();
        props.put("Name", getName());
        props.put("Owner", getOwner());
        props.put("Encoding", getEncoding());
        props.put("Collate", getCollate());
        props.put("ctype", getCtype());
        props.put("AccessPrivileges", getAccessPrivileges());
        props.put("Version", getVersion() );
        return props;
    }

    public String getQualifiedName ( ) {

        return Name;
    }
}
