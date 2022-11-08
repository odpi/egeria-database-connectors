/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.repository.caching.database.helpers;

import java.util.Arrays;
import java.util.List;

public class SupportedTypes {

    // TODO config?
    // this should be a natural to the technology separator character to be used to separate elements in a name.
    public static final String SEPARATOR_CHAR = ".";


    public static final String CONNECTION = "Connection";
    public static final String CONNECTOR_TYPE = "ConnectorType";
    public static final String ENDPOINT = "Endpoint";
    public static final String CONNECTION_ENDPOINT = "ConnectionEndpoint";
    public static final String CONNECTION_CONNECTOR_TYPE = "ConnectionConnectorType";
    public static final String CONNECTION_TO_ASSET = "ConnectionToAsset";
    public static final String DATABASE = "Database";

    public static final String RELATIONAL_DB_SCHEMA_TYPE = "RelationalDBSchemaType";
    // relationship
    public static final String DATA_CONTENT_FOR_DATASET = "DataContentForDataSet";
    // relationship
    public static final String ASSET_SCHEMA_TYPE = "AssetSchemaType";
    //relationship
    public static final String ATTRIBUTE_FOR_SCHEMA = "AttributeForSchema";

    public static final String TABLE = "RelationalTable";

    public static final String COLUMN = "RelationalColumn";

    public static final String RELATIONAL_TABLE_TYPE = "RelationalTableType";

    public static final String SCHEMA_ATTRIBUTE_TYPE = "SchemaAttributeType";

    public static final String RELATIONAL_COLUMN_TYPE = "RelationalColumnType";
    // relationship
    public static final String NESTED_SCHEMA_ATTRIBUTE = "NestedSchemaAttribute";
    // classification
    public static final String TYPE_EMBEDDED_ATTRIBUTE = "TypeEmbeddedAttribute";

    public static final String CALCULATED_VALUE = "CalculatedValue";
    public static final List<String> supportedTypeNames = Arrays.asList(new String[]{
            // entity types
            "Asset", // super type of Database
            "Referenceable", // super type of the others
            "OpenMetadataRoot", // super type of referenceable
            "SchemaAttribute",
            "SchemaElement",
            "ComplexSchemaType",
            "SchemaType",

            CONNECTION,
            CONNECTOR_TYPE,
            ENDPOINT,
            RELATIONAL_TABLE_TYPE,
            RELATIONAL_COLUMN_TYPE,
            DATABASE,
            RELATIONAL_DB_SCHEMA_TYPE,
            TABLE,
            COLUMN,
            // relationship types
            CONNECTION_ENDPOINT,
            CONNECTION_CONNECTOR_TYPE,
            CONNECTION_TO_ASSET,
            ASSET_SCHEMA_TYPE,
            ATTRIBUTE_FOR_SCHEMA,
            NESTED_SCHEMA_ATTRIBUTE,
            DATA_CONTENT_FOR_DATASET,
            SCHEMA_ATTRIBUTE_TYPE,
            // classification types
            TYPE_EMBEDDED_ATTRIBUTE,
            CALCULATED_VALUE
    });
}
