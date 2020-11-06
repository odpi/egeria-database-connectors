/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.datastore.postgres;

import org.odpi.openmetadata.frameworks.connectors.ConnectorProviderBase;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;

public class PostgresDatabaseProvider extends ConnectorProviderBase
{
    static final String  connectorTypeGUID = "f5283503-eee9-4543-b7fd-17cd10b79931";
    static final String  connectorTypeName = "Postgres Database Connector";
    static final String  connectorTypeDescription = "Connector supports Postgres Database instance";




   /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * store implementation.
     */
    public PostgresDatabaseProvider()
    {
        Class<?> connectorClass = PostgresDatabaseProvider.class;

        super.setConnectorClassName(connectorClass.getName());


        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(this.getClass().getName());

        super.connectorTypeBean = connectorType;
    }
}
