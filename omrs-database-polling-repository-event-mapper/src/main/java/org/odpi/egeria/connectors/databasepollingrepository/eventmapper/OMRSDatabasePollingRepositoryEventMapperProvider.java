/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.databasepollingrepository.eventmapper;

import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnectorProviderBase;

import java.util.ArrayList;
import java.util.List;

/**
 * In the Open Connector Framework (OCF), a ConnectorProvider is a factory for a specific type of connector.
 * The ApacheAtlasOMRSRepositoryEventMapperProvider is the connector provider for the ApacheAtlasOMRSRepositoryEventMapperProvider.
 * It extends OMRSRepositoryEventMapperProviderBase which in turn extends the OCF ConnectorProviderBase.
 * ConnectorProviderBase supports the creation of connector instances.
 *
 * The ApacheAtlasOMRSRepositoryEventMapperProvider must initialize ConnectorProviderBase with the Java class
 * name of the OMRS Connector implementation (by calling super.setConnectorClassName(className)).
 * Then the connector provider will work.
 */
abstract public class OMRSDatabasePollingRepositoryEventMapperProvider extends OMRSRepositoryConnectorProviderBase {

    static final String QUALIFIED_NAME_PREFIX = "qualifiedNamePrefix";

    static final String REFRESH_TIME_INTERVAL = "refreshTimeInterval";

    static final String CATALOG_NAME = "CatalogName";
    static final String DATABASE_NAME = "DatabaseName";

    static final String SEND_POLL_EVENTS = "sendPollEvents";

    /**
     * If this is set then we use this as the endpoint address (e.g. the JDBC URL)
     * If it is not set then, no connection is associated with the asset
     */
    static final String ENDPOINT_ADDRESS = "endpointAddress";

    static final String SEND_SCHEMA_TYPES_AS_ENTITIES = "sendSchemaTypesAsEntities";

    /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * OMRS Connector implementation.
     */
    public OMRSDatabasePollingRepositoryEventMapperProvider() {
        Class<?> connectorClass = OMRSDatabasePollingRepositoryEventMapper.class;
        super.setConnectorClassName(connectorClass.getName());
        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());

        connectorType.setConnectorProviderClassName(this.getClass().getName());

        List<String> knownConfigProperties = new ArrayList<>();
        knownConfigProperties.add(QUALIFIED_NAME_PREFIX);
        knownConfigProperties.add(ENDPOINT_ADDRESS);
        knownConfigProperties.add(REFRESH_TIME_INTERVAL);
        knownConfigProperties.add(DATABASE_NAME);
        knownConfigProperties.add(CATALOG_NAME);
        knownConfigProperties.add(SEND_POLL_EVENTS);

        connectorType.setRecognizedConfigurationProperties(knownConfigProperties);

        super.setConnectorTypeProperties(connectorType);
    }

}
