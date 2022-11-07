/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.integration.jdbc.transfer.customization;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String INCLUDE_SCHEMA_NAMES = "includeSchemaNames";
    public static final String EXCLUDE_SCHEMA_NAMES = "excludeSchemaNames";
    public static final String INCLUDE_TABLE_NAMES = "includeTableNames";
    public static final String EXCLUDE_TABLE_NAMES = "excludeTableNames";
    public static final String INCLUDE_COLUMN_NAMES = "includeColumnNames";
    public static final String EXCLUDE_COLUMN_NAMES = "excludeColumnNames";
    public static final List<String> INCLUSION_AND_EXCLUSION_NAMES = Arrays.asList(INCLUDE_SCHEMA_NAMES, INCLUDE_TABLE_NAMES,
            INCLUDE_COLUMN_NAMES, EXCLUDE_SCHEMA_NAMES, EXCLUDE_TABLE_NAMES, EXCLUDE_COLUMN_NAMES);
}
