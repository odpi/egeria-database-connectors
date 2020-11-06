/* SPDX-License-Identifier: Apache 2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties;

import java.util.HashMap;
import java.util.Map;

public class PostgresPrimaryKey
{
    private final String constraint_schema;
    private final String constraint_name;
    private final String constraint_catalog;
    private final String table_catalog;
    private final String table_schema;
    private final String table_name;
    private final String constraint_type;
    private final String is_deferrable;
    private final String initially_deferred;
    private final String enforced;
    private final String column_name;
    private final String ordinal_position;
    private final String column_default;
    private final String is_nullable;
    private final String data_type;
    private final String character_maximum_length;
    private final String character_octet_length;
    private final String numeric_precision;
    private final String numeric_precision_radix;
    private final String numeric_scale;
    private final String datetime_precision;
    private final String interval_type;
    private final String interval_precision;
    private final String character_set_catalog;
    private final String character_set_schema;
    private final String character_set_name;
    private final String collation_catalog;
    private final String collation_schema;
    private final String collation_name;
    private final String domain_catalog;
    private final String domain_schema;
    private final String domain_name;
    private final String udt_catalog;
    private final String udt_schema;
    private final String udt_name;
    private final String scope_catalog;
    private final String scope_schema;
    private final String scope_name;
    private final String maximum_cardinality;
    private final String dtd_identifier;
    private final String is_self_referencing;
    private final String is_identity;
    private final String identity_generation;
    private final String identity_start;
    private final String identity_increment;
    private final String identity_maximum;
    private final String identity_minimum;
    private final String identity_cycle;
    private final String is_generated;
    private final String generation_expression;
    private final String is_updatable;



    public PostgresPrimaryKey(String constraint_schema, String constraint_name, String constraint_catalog, String table_catalog, String table_schema, String table_name, String constraint_type, String is_deferrable, String initially_deferred, String enforced, String column_name, String ordinal_position, String column_default, String is_nullable, String data_type, String character_maximum_length, String character_octet_length, String numeric_precision, String numeric_precision_radix, String numeric_scale, String datetime_precision, String interval_type, String interval_precision, String character_set_catalog, String character_set_schema, String character_set_name, String collation_catalog, String collation_schema, String collation_name, String domain_catalog, String domain_schema, String domain_name, String udt_catalog, String udt_schema, String udt_name, String scope_catalog, String scope_schema, String scope_name, String maximum_cardinality, String dtd_identifier, String is_self_referencing, String is_identity, String identity_generation, String identity_start, String identity_increment, String identity_maximum, String identity_minimum, String identity_cycle, String is_generated, String generation_expression, String is_updatable) {
        this.constraint_schema = constraint_schema;
        this.constraint_name = constraint_name;
        this.constraint_catalog = constraint_catalog;
        this.table_catalog = table_catalog;
        this.table_schema = table_schema;
        this.table_name = table_name;
        this.constraint_type = constraint_type;
        this.is_deferrable = is_deferrable;
        this.initially_deferred = initially_deferred;
        this.enforced = enforced;
        this.column_name = column_name;
        this.ordinal_position = ordinal_position;
        this.column_default = column_default;
        this.is_nullable = is_nullable;
        this.data_type = data_type;
        this.character_maximum_length = character_maximum_length;
        this.character_octet_length = character_octet_length;
        this.numeric_precision = numeric_precision;
        this.numeric_precision_radix = numeric_precision_radix;
        this.numeric_scale = numeric_scale;
        this.datetime_precision = datetime_precision;
        this.interval_type = interval_type;
        this.interval_precision = interval_precision;
        this.character_set_catalog = character_set_catalog;
        this.character_set_schema = character_set_schema;
        this.character_set_name = character_set_name;
        this.collation_catalog = collation_catalog;
        this.collation_schema = collation_schema;
        this.collation_name = collation_name;
        this.domain_catalog = domain_catalog;
        this.domain_schema = domain_schema;
        this.domain_name = domain_name;
        this.udt_catalog = udt_catalog;
        this.udt_schema = udt_schema;
        this.udt_name = udt_name;
        this.scope_catalog = scope_catalog;
        this.scope_schema = scope_schema;
        this.scope_name = scope_name;
        this.maximum_cardinality = maximum_cardinality;
        this.dtd_identifier = dtd_identifier;
        this.is_self_referencing = is_self_referencing;
        this.is_identity = is_identity;
        this.identity_generation = identity_generation;
        this.identity_start = identity_start;
        this.identity_increment = identity_increment;
        this.identity_maximum = identity_maximum;
        this.identity_minimum = identity_minimum;
        this.identity_cycle = identity_cycle;
        this.is_generated = is_generated;
        this.generation_expression = generation_expression;
        this.is_updatable = is_updatable;
    }


    public Map<String,String> getProperties()
    {
        Map<String, String> props = new HashMap<>();

        props.put("constraint_schema", getConstraint_schema());
        props.put("constraint_name", getConstraint_name());
        props.put("constraint_catalog", getConstraint_catalog());
        props.put("table_catalog", getTable_catalog());
        props.put("table_schema", getTable_schema());
        props.put("table_name", getTable_name());
        props.put("constraint_type", getConstraint_type());
        props.put("is_deferrable", getIs_deferrable());
        props.put("initially_deferred", getInitially_deferred());
        props.put("enforced", getEnforced());
        props.put("column_name", getColumn_name());
        props.put("ordinal_position", getOrdinal_position());
        props.put("column_default", getColumn_default());
        props.put("is_nullable", getIs_nullable());
        props.put("data_type", getData_type());
        props.put("character_maximum_length", getCharacter_maximum_length());
        props.put("character_octet_length", getCharacter_octet_length());
        props.put("numeric_precision", getNumeric_precision());
        props.put("numeric_precision_radix", getNumeric_precision());
        props.put("numeric_scale", getNumeric_scale());
        props.put("datetime_precision", getDatetime_precision());
        props.put("interval_type", getInterval_type());
        props.put("interval_precision", getInterval_precision());
        props.put("character_set_catalog", getCharacter_set_catalog());
        props.put("character_set_schema", getCharacter_set_schema());
        props.put("character_set_name", getCharacter_set_name());
        props.put("collation_catalog", getCollation_catalog());
        props.put("collation_schema", getCollation_schema());
        props.put("collation_name", getCollation_name());
        props.put("domain_catalog", getDomain_catalog());
        props.put("domain_schema", getDomain_schema());
        props.put("domain_name", getDomain_name());
        props.put("udt_catalog", getUdt_catalog());
        props.put("udt_schema", getUdt_schema());
        props.put("udt_name", getUdt_name());
        props.put("scope_catalog", getScope_catalog());
        props.put("scope_schema", getScope_schema());
        props.put("scope_name", getScope_name());
        props.put("maximum_cardinality", getMaximum_cardinality());
        props.put("dtd_identifier", getDtd_identifier());
        props.put("is_self_referencing", getIs_self_referencing());
        props.put("is_identity", getIs_identity());
        props.put("identity_generation", getIdentity_generation());
        props.put("identity_start", getIdentity_start());
        props.put("identity_increment", getIdentity_increment());
        props.put("identity_maximum", getIdentity_maximum());
        props.put("identity_minimum", getIdentity_minimum());
        props.put("identity_cycle", getIdentity_cycle());
        props.put("is_generated", getIs_generated());
        props.put("generation_expression", getGeneration_expression() );
        props.put("is_updatable", getIs_updatable());

        return props;
    }

    public String getConstraint_schema() {
        return constraint_schema;
    }

    public String getConstraint_name() {
        return constraint_name;
    }

    public String getConstraint_catalog() {
        return constraint_catalog;
    }

    public String getTable_catalog() {
        return table_catalog;
    }

    public String getTable_schema() {
        return table_schema;
    }

    public String getTable_name() {
        return table_name;
    }

    public String getConstraint_type() {
        return constraint_type;
    }

    public String getIs_deferrable() {
        return is_deferrable;
    }

    public String getInitially_deferred() {
        return initially_deferred;
    }

    public String getEnforced() {
        return enforced;
    }

    public String getColumn_name() {
        return column_name;
    }

    public String getOrdinal_position() {
        return ordinal_position;
    }

    public String getColumn_default() {
        return column_default;
    }

    public String getIs_nullable() {
        return is_nullable;
    }

    public String getData_type() {
        return data_type;
    }

    public String getCharacter_maximum_length() {
        return character_maximum_length;
    }

    public String getCharacter_octet_length() {
        return character_octet_length;
    }

    public String getNumeric_precision() {
        return numeric_precision;
    }

    public String getNumeric_precision_radix() {
        return numeric_precision_radix;
    }

    public String getNumeric_scale() {
        return numeric_scale;
    }

    public String getDatetime_precision() {
        return datetime_precision;
    }

    public String getInterval_type() {
        return interval_type;
    }

    public String getInterval_precision() {
        return interval_precision;
    }

    public String getCharacter_set_catalog() {
        return character_set_catalog;
    }

    public String getCharacter_set_schema() {
        return character_set_schema;
    }

    public String getCharacter_set_name() {
        return character_set_name;
    }

    public String getCollation_catalog() {
        return collation_catalog;
    }

    public String getCollation_schema() {
        return collation_schema;
    }

    public String getCollation_name() {
        return collation_name;
    }

    public String getDomain_catalog() {
        return domain_catalog;
    }

    public String getDomain_schema() {
        return domain_schema;
    }

    public String getDomain_name() {
        return domain_name;
    }

    public String getUdt_catalog() {
        return udt_catalog;
    }

    public String getUdt_schema() {
        return udt_schema;
    }

    public String getUdt_name() {
        return udt_name;
    }

    public String getScope_catalog() {
        return scope_catalog;
    }

    public String getScope_schema() {
        return scope_schema;
    }

    public String getScope_name() {
        return scope_name;
    }

    public String getMaximum_cardinality() {
        return maximum_cardinality;
    }

    public String getDtd_identifier() {
        return dtd_identifier;
    }

    public String getIs_self_referencing() {
        return is_self_referencing;
    }

    public String getIs_identity() {
        return is_identity;
    }

    public String getIdentity_generation() {
        return identity_generation;
    }

    public String getIdentity_start() {
        return identity_start;
    }

    public String getIdentity_increment() {
        return identity_increment;
    }

    public String getIdentity_maximum() {
        return identity_maximum;
    }

    public String getIdentity_minimum() {
        return identity_minimum;
    }

    public String getIdentity_cycle() {
        return identity_cycle;
    }

    public String getIs_generated() {
        return is_generated;
    }

    public String getGeneration_expression() {
        return generation_expression;
    }

    public String getIs_updatable() {
        return is_updatable;
    }

}
