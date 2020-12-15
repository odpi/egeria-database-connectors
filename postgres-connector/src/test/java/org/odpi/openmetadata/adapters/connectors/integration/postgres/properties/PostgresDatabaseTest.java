package org.odpi.openmetadata.adapters.connectors.integration.postgres.properties;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDatabaseTest {


    @Test
    void getName() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");

        assertEquals ( database.getName(), "name");
    }

    @Test
    void getOwner() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");

        assertEquals ( database.getOwner(),"owner");
    }

    @Test
    void getEncoding() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        assertEquals ( database.getEncoding(),"encoding");
    }

    @Test
    void getCollate() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        assertEquals ( database.getCollate(), "collate");
    }

    @Test
    void getCtype() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        assertEquals ( database.getCtype(), "ctype");

    }

    @Test
    void getAccessPrivileges() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        assertEquals ( database.getAccessPrivileges(), "accessPrivileges" );
    }

    @Test
    void getVersion() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        assertEquals ( database.getVersion(), "version" );

    }

    @Test
    void getProperties() {
        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("name","name");
        testProps.put("owner","owner");
        testProps.put("ctype", "ctype" );
        testProps.put("version", "version");
        testProps.put("collate", "collate");
        testProps.put("encoding", "encoding" );
        testProps.put("accessPrivileges", "accessPrivileges" );
        testProps.put("instance", "instance" );

        Map<String, String> props = database.getProperties();
        assertEquals( props.size(), 8);
        assertTrue( props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey()))));
    }

    @Test
    void getQualifiedName() {

        PostgresDatabase database = new PostgresDatabase("name",
                "owner",
                "encoding",
                "collate",
                "ctype",
                "accessPrivileges",
                "version",
                "instance");
        String qName =  database.getInstance() + "::"  + database.getOwner() + "::" + database.getName();
        assertEquals(database.getQualifiedName(), qName );
    }
}