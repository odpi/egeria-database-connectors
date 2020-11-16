package org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PostgresTableTest {

    @Test
    void getTable_catalog() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_catalog(), "table_catalog");
    }

    @Test
    void getTable_schema() {

        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_schema(), "table_schema");

    }

    @Test
    void getTable_name() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_name(), "table_name");
    }

    @Test
    void getTable_type() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getTable_type(), "table_type");
    }

    @Test
    void getSelf_referencing_column_name() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getSelf_referencing_column_name(), "self_referencing_column_name");
    }

    @Test
    void getReference_generation() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getReference_generation(), "reference_generation");

    }

    @Test
    void getUser_defined_type_catalog() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_catalog(), "reference_generation");
    }

    @Test
    void getUser_defined_type_schema() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_schema(), "user_defined_type_schema");

    }

    @Test
    void getUser_defined_type_name() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getUser_defined_type_name(), "user_defined_type_name");
    }

    @Test
    void getIs_insertable_into() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getIs_insertable_into(), "is_insertable_into");
    }

    @Test
    void getIs_typed() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getIs_typed(), "is_typed");
    }

    @Test
    void getCommit_action() {

        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        assertEquals( table.getCommit_action(), "commit_action");

    }

    @Test
    void getProperties() {
        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("owner","owner");
        testProps.put("ctype", "ctype" );
        testProps.put("version", "version");
        testProps.put("collate", "collate");
        testProps.put("encoding", "encoding" );
        testProps.put("name","name");
        testProps.put("accessPrivileges", "accessPrivileges" );

        Map<String, String> props = table.getProperties();
        assertEquals( props.size(), 7);

        assertTrue( props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey()))) );

    }

    @Test
    void getQualifiedName() {

        PostgresTable table = new PostgresTable("table_catalog",
                "table_schema",
                "table_name",
                "table_type",
                "self_referencing_column_name",
                "reference_generation",
                "user_defined_type_catalog",
                "user_defined_type_schema",
                "user_defined_type_name",
                "is_insertable_into",
                "is_typed",
                "commit_action");

        String qName = new StringBuilder().append(table.getTable_catalog()).append(".")
                                            .append(table.getTable_schema()).append(".")
                                            .append(table.getTable_type()).append(".")
                                            .append(table.getTable_name()).toString();

        assertEquals( table.getQualifiedName(), qName);

    }
}