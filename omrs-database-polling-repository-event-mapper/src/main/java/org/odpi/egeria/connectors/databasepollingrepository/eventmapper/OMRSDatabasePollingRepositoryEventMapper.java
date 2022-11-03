/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.databasepollingrepository.eventmapper;

import org.odpi.egeria.connectors.databasepollingrepository.ConnectorColumn;
import org.odpi.egeria.connectors.databasepollingrepository.ConnectorTable;
import org.odpi.egeria.connectors.databasepollingrepository.auditlog.DbPollingOMRSAuditCode;
import org.odpi.egeria.connectors.databasepollingrepository.auditlog.DbPollingOMRSErrorCode;

import org.odpi.egeria.connectors.databasepollingrepository.helpers.ExceptionHelper;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;


import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * OMRSDatabasePollingRepositoryEventMapper supports the event mapper function for a Hive metastore used as an open metadata repository.
 *
 * This class is an implementation of an OMRS event mapper, it polls for content in Hive metastore and puts
 * that content into an embedded Egeria repository. It then (if configured to send batch events) extracts the entities and relationships
 * from the embedded repository and sends a batch event for
 * 1) for the asset Entities and relationships
 * 2) for each RelationalTable, it's RelationalColumns and associated relationships
 */
abstract public class OMRSDatabasePollingRepositoryEventMapper extends OMRSRepositoryEventMapperBase
{


    /**
     * Running field is a thread safe indicator that the thread is running. So stop the thread set the running flag to false.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * map to help manage type guids, by keeping a map for type name to guid.
     */
    private Map<String, String> typeNameToGuidMap = null;

    /**
     * UserId associated with this connector
     */
    private String userId = null;
    /**
     * Default polling refresh interval in milliseconds.
     */
    private int refreshInterval = 5000;
    private String qualifiedNamePrefix = "";
    protected String metadataCollectionId = null;
    protected String metadataCollectionName = null;
    protected OMRSMetadataCollection metadataCollection = null;

    private String repositoryName = null;
    private String catName = "spark";
    private String dbName = "default";
    private String metadata_store_userId = null;
    private String metadata_store_password = null;

    private boolean sendPollEvents = false;

    private boolean useSSL = false;
    private boolean sendEntitiesForSchemaType = false;

    private String configuredEndpointAddress = null;

    private PollingThread pollingThread;
    private String databaseGUID;
    private final String className = this.getClass().getName();

    /**
     * Default constructor
     */
    public OMRSDatabasePollingRepositoryEventMapper() {
        super();
    }

    public String getUserId() {
        return userId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public void start() throws ConnectorCheckedException {

        super.start();

        final String methodName = "start";
        // synchronise in case the atart occurs while the thread is running accessing the private varioables.
        // this synchronisation should ensure that all the config is updated together before the polling thread accesses them
        synchronized (this) {
            repositoryName = this.repositoryConnector.getRepositoryName();
            auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_STARTING.getMessageDefinition());

            if (!(repositoryConnector instanceof CachingOMRSRepositoryProxyConnector)) {
                ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.EVENT_MAPPER_IMPROPERLY_INITIALIZED, methodName, null, repositoryConnector.getServerName());
            }

            this.repositoryHelper = this.repositoryConnector.getRepositoryHelper();

            Map<String, Object> configurationProperties = connectionProperties.getConfigurationProperties();
            this.userId = connectionProperties.getUserId();
            if (this.userId == null) {
                // default
                this.userId = "OMAGServer";
            }
            if (configurationProperties != null) {
                extractConfigurationProperties(configurationProperties);

            }
            if (metadataCollection == null) {
                try {
                    connectTo3rdParty();
                } catch (RepositoryErrorException e) {
                    ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.FAILED_TO_START_CONNECTOR, methodName, null);
                }
            }
        }

        this.pollingThread = new PollingThread();
        pollingThread.start();
    }

    protected abstract void connectTo3rdParty() throws RepositoryErrorException, ConnectorCheckedException;
    protected abstract List<ConnectorTable> getTablesFrom3rdParty(String catName, String dbName, String baseCanonicalName);


    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public void disconnect() throws ConnectorCheckedException {
        super.disconnect();
        final String methodName = "disconnect";
        pollingThread.stop();
        auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_SHUTDOWN.getMessageDefinition(repositoryConnector.getServerName()));
    }

    /**
     * Extract Egeria configuration properties into instance variables
     *
     * @param configurationProperties map of Egeria configuration variables.
     */
    private void extractConfigurationProperties(Map<String, Object> configurationProperties) {
        Integer configuredRefreshInterval = (Integer) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.REFRESH_TIME_INTERVAL);
        if (configuredRefreshInterval != null) {
            refreshInterval = configuredRefreshInterval * 1000;
        }
        String configuredQualifiedNamePrefix = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.QUALIFIED_NAME_PREFIX);
        if (configuredQualifiedNamePrefix != null) {
            qualifiedNamePrefix = configuredQualifiedNamePrefix;
        }
        String configuredCatName = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.CATALOG_NAME);
        if (configuredCatName != null) {
            catName = configuredCatName;
        }
        String configuredDBName = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.DATABASE_NAME);
        if (configuredDBName != null) {
            dbName = configuredDBName;
        }
        String configuredMetadataStoreUserId = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.METADATA_STORE_USER);
        if (configuredMetadataStoreUserId != null) {
            metadata_store_userId = configuredMetadataStoreUserId;
        }
        String configuredMetadataStorePassword = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.METADATA_STORE_PASSWORD);
        if (configuredMetadataStorePassword != null) {
            metadata_store_password = configuredMetadataStorePassword;
        }

        Boolean configuredSendPollEvents = (Boolean) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.SEND_POLL_EVENTS);
        if (configuredSendPollEvents != null) {
            sendPollEvents = configuredSendPollEvents;
        }
        Boolean configuredUseSSL = (Boolean) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.USE_SSL);
        if (configuredUseSSL != null) {
            useSSL = configuredUseSSL;
        }
        configuredEndpointAddress = (String) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.ENDPOINT_ADDRESS);

        Boolean configuredSendEntitiesForSchemaType = (Boolean) configurationProperties.get(OMRSDatabasePollingRepositoryEventMapperProvider.SEND_SCHEMA_TYPES_AS_ENTITIES);
        if (configuredSendEntitiesForSchemaType != null) {
            sendEntitiesForSchemaType = true;
        }

    }


    /**
     * Class to poll for Hive Meta store content
     */
    public class PollingThread implements Runnable {
        Thread worker = null;
        List<Relationship> aboveTableRelationshipListToStore = new ArrayList<>();
        List<EntityDetail> aboveTableEntityListToStore = new ArrayList<>();
        Map<String, List<EntityDetail>> qualifiedTableNameToEntityMap = new HashMap<>();
        Map<String, List<Relationship>> qualifiedTableNameToRelationshipMap = new HashMap<>();

        CachedRepositoryAccessor cachedRepositoryAccessor = null;
        String baseCanonicalName = null;

        String relationalDBTypeGuid = null;

        MapperHelper mapperHelper = null;

        /**
         * Start the thread
         */
        void start() {
            Thread worker = new Thread(this);
            worker.start();
        }

        /**
         * Stop the thread
         */

        void stop() {
            if (!running.compareAndSet(true, false)) {
                auditLog.logMessage("stop", DbPollingOMRSAuditCode.POLLING_THREAD_INFO_ALREADY_STOPPED.getMessageDefinition());
            }
        }

        @Override
        public void run() {

            final String methodName = "run";
            if (running.compareAndSet(false, true)) {
                while (running.get()) {
                    try {
                         // synchronise the processing to ensure that a start does not change the instance variables under us.
                        synchronized (this) {
                            mapperHelper = new MapperHelper(repositoryHelper,
                                    userId,
                                    metadataCollectionId,
                                    repositoryName,
                                    metadataCollectionName, qualifiedNamePrefix);

                            getRequiredTypes();
                            // connect to the 3rd party technoilogy
                            connectTo3rdParty();
                            // reset the variables used to accumulate state
                            cachedRepositoryAccessor = new CachedRepositoryAccessor(userId, repositoryConnector.getServerName(), metadataCollection);
                            aboveTableEntityListToStore = new ArrayList<>();
                            aboveTableRelationshipListToStore = new ArrayList<>();
                            qualifiedTableNameToEntityMap = new HashMap<>();
                            qualifiedTableNameToRelationshipMap = new HashMap<>();

                            // populate the above lists with the database and schema entities and relationships

                            baseCanonicalName = catName + SupportedTypes.SEPARATOR_CHAR + dbName;
                            // collect the entities and relationships above the table(s)
                            collectEntitiesAndRelationshipsAboveTable();

                            // create ConnectionTables. This method uses the Hive client.
                            // In the future if this event mapper is copied for other technologies - this is the method that needs to be reworking to
                            // use the new technology
                            List<ConnectorTable> connectorTables = getConnectionTablesAndColumnsFrom3rdParty();
                            // Subsequent processing is not Hive specific.
                            convertToConnectorTablesToEntitiesAndRelationships(connectorTables);
                            refreshRepositoryAndSendBatchEvent();
                        }
                        // come out of synchronization when waiting.
                        //  wait the polling interval.
                        auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_PRE_WAIT.getMessageDefinition());
                        try {
                            Thread.sleep(refreshInterval);
                            auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_POST_WAIT.getMessageDefinition());
                        } catch (InterruptedException e) {
                            // should not happen as there is only one thread
                            // if it happens then continue in the while
                            auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_INTERRUPTED_EXCEPTION.getMessageDefinition());
                        }


                    } catch (ConnectorCheckedException e) {
                        String msg = "No Exception message";
                        if (e.getMessage() != null) {
                            msg = e.getMessage();
                        }
                        Throwable cause = e.getCause();
                        if (cause == null) {
                            auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_GOT_AN_EXCEPTION.getMessageDefinition(msg));
                        } else {
                            String causeMsg = "No cause message";
                            if (cause.getMessage() != null) {
                                causeMsg = cause.getMessage();
                            }
                            auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_GOT_AN_EXCEPTION_WITH_CAUSE.getMessageDefinition(msg, causeMsg));
                        }
                    } catch (Exception e) {
                        Throwable cause = e.getCause();
                        // catch everything else
                        String msg = "No Exception message";
                        if (e.getMessage() != null) {
                            msg = e.getMessage();
                        }
                        String causeMsg = "No cause message";
                        if (cause.getMessage() != null) {
                            causeMsg = cause.getMessage();
                        }

                        auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_POLL_LOOP_GOT_AN_EXCEPTION_WITH_CAUSE.getMessageDefinition(msg, causeMsg));
                    } finally {
                        // stop the thread if we came out of the loop.
                        this.stop();
                    }
                }
            }
        }

        /**
         * Collect the Entities and relationships above the tables(s)
         *
         * @throws ConnectorCheckedException connector exception
         */
        private void collectEntitiesAndRelationshipsAboveTable() throws ConnectorCheckedException {
            String methodName = "collectEntitiesAndRelationshipsAboveTable";

            // Create database

            EntityDetail databaseEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                    SupportedTypes.DATABASE,
                    baseCanonicalName,
                    baseCanonicalName,
                    null,
                    false);
            databaseGUID = databaseEntity.getGUID();
            saveEntityReferenceCopy(databaseEntity);

            createConnectionOrientatedEntities(baseCanonicalName, databaseEntity);

            // create RelationalDBType
            EntityDetail relationalDBTypeEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                    SupportedTypes.RELATIONAL_DB_SCHEMA_TYPE,
                    dbName + SupportedTypes.SEPARATOR_CHAR + "schemaType",
                    baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + "schemaType",
                    null,
                    false);
            saveEntityReferenceCopy(relationalDBTypeEntity);
            relationalDBTypeGuid = relationalDBTypeEntity.getGUID();

            // create Relationship

            aboveTableRelationshipListToStore.add(mapperHelper.createReferenceRelationship(SupportedTypes.ASSET_SCHEMA_TYPE,
                    databaseGUID,
                    SupportedTypes.DATABASE,
                    relationalDBTypeEntity.getGUID(),
                    SupportedTypes.RELATIONAL_DB_SCHEMA_TYPE));

        }

        /**
         * refresh the repository with the entities and relationships and then send batch events, one per table and then one for the other entities and relationships above the table
         *
         * @throws ConnectorCheckedException connector exception
         */
        private void refreshRepositoryAndSendBatchEvent() throws ConnectorCheckedException {
            refreshRepositoryAndSendBatchEventForAboveTable();
            refreshRepositoryAndSendBatchEventForEachTable();
        }

        /**
         * refresh the repository with the entities and relationships and then send batch events, above the tables
         *
         * @throws ConnectorCheckedException connector exception
         */
        private void refreshRepositoryAndSendBatchEventForAboveTable() throws ConnectorCheckedException {
            List<String> entityGUIDs = new ArrayList<>();
            List<String> relationshipGUIDs = new ArrayList<>();
            // above table entities and relationships
            for (EntityDetail entity : aboveTableEntityListToStore) {
                cachedRepositoryAccessor.saveEntityReferenceCopyToStore(entity);
                entityGUIDs.add(entity.getGUID());
            }
            for (Relationship relationship : aboveTableRelationshipListToStore) {
                cachedRepositoryAccessor.saveRelationshipReferenceCopyToStore(relationship);
                relationshipGUIDs.add(relationship.getGUID());
            }
            // get the entities and relationships from the repository
            List<Relationship> relationshipList = new ArrayList<>();
            List<EntityDetail> entityList = new ArrayList<>();
            for (String guid : entityGUIDs) {
                entityList.add(cachedRepositoryAccessor.getEntityDetailFromStore(guid));
            }
            for (String guid : relationshipGUIDs) {
                relationshipList.add(cachedRepositoryAccessor.getRelationshipFromStore(guid));
            }

            if (sendPollEvents) {
                issueBatchEvent(relationshipList, entityList);
            }

        }

        /**
         * refresh the repository with the entities and relationships and then send batch events, for each table
         *
         * @throws ConnectorCheckedException connector exception
         */
        private void refreshRepositoryAndSendBatchEventForEachTable() throws ConnectorCheckedException {
            for (String qualifiedName : qualifiedTableNameToEntityMap.keySet()) {
                List<String> entityGUIDs = new ArrayList<>();
                List<String> relationshipGUIDs = new ArrayList<>();
                List<EntityDetail> entities = qualifiedTableNameToEntityMap.get(qualifiedName);
                List<Relationship> relationships = qualifiedTableNameToRelationshipMap.get(qualifiedName);
                for (EntityDetail entity : entities) {
                    cachedRepositoryAccessor.saveEntityReferenceCopyToStore(entity);
                    entityGUIDs.add(entity.getGUID());
                }
                for (Relationship relationship : relationships) {
                    cachedRepositoryAccessor.saveRelationshipReferenceCopyToStore(relationship);
                    relationshipGUIDs.add(relationship.getGUID());
                }
                // get the entities and relationships from the repository
                List<Relationship> relationshipList = new ArrayList<>();
                List<EntityDetail> entityList = new ArrayList<>();

                for (String guid : entityGUIDs) {
                    entityList.add(cachedRepositoryAccessor.getEntityDetailFromStore(guid));
                }
                for (String guid : relationshipGUIDs) {
                    relationshipList.add(cachedRepositoryAccessor.getRelationshipFromStore(guid));
                }

                if (sendPollEvents) {
                    issueBatchEvent(relationshipList, entityList);
                }
            }
        }

        /**
         * Check that the repository supports the required types. This is done in a loop with small delays as the types are added asynchonously, so we need to loop until we get the
         * required types
         *
         * @throws ConnectorCheckedException connector exception
         */
        private void getRequiredTypes() throws ConnectorCheckedException {
            String methodName = "getRequiredTypes";
            final int supportedCount = SupportedTypes.supportedTypeNames.size();

            int typesAvailableCount = 0;
            int retryCount = 0;
            while ((typesAvailableCount != supportedCount)) {
                auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_ACQUIRING_TYPES_LOOP.getMessageDefinition(typesAvailableCount + "", supportedCount + "", retryCount + ""));
                // only come out the while loop when we can get all of the supported types in one iteration.
                typesAvailableCount = 0;
                if (typeNameToGuidMap == null) {
                    typeNameToGuidMap = new HashMap<>();
                }
                // populate the type name to guid map
                for (String typeName : SupportedTypes.supportedTypeNames) {

                    TypeDef typeDef = repositoryHelper.getTypeDefByName("OMRSDatabasePollingRepositoryEventMapper",
                            typeName);
                    if (typeDef != null) {
                        auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_ACQUIRING_TYPES_LOOP_FOUND_TYPE.getMessageDefinition(typeName));
                        typeNameToGuidMap.put(typeName, typeDef.getGUID());
                        typesAvailableCount++;
                    }
                }
                if (typesAvailableCount < supportedCount) {
                    //delay for 1 second and then retry

                    try {
                        Thread.sleep(1000);  // TODO Should this be in configuration?
                        retryCount++;
                    } catch (InterruptedException e) {
                        // should not happen as there is only one thread
                        // if it does happen it would result in a lower duration for the sleep
                        //
                        // Increment the retry count, in case this happens everytime
                        retryCount++;
                        auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_ACQUIRING_TYPES_LOOP_INTERRUPTED_EXCEPTION.getMessageDefinition());
                    }
                } else if (typesAvailableCount == supportedCount) {
                    // log to say we have all the types we need
                    auditLog.logMessage(methodName, DbPollingOMRSAuditCode.EVENT_MAPPER_ACQUIRED_ALL_TYPES.getMessageDefinition());

                }

                if (retryCount == 20) { // TODO  Should this be in configuration?
                    ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.EVENT_MAPPER_CANNOT_GET_TYPES, methodName, null);
                }
            }
        }

        /**
         * this method issues the HMS client calls to get the table content
         *
         * @return a list of Connector tables
         * @throws ConnectorCheckedException connector checked exception
         */
        public List<ConnectorTable> getConnectionTablesAndColumnsFrom3rdParty() throws ConnectorCheckedException {
            String methodName = "getConnectionTablesAndColumnsFrom3rdParty";
            List<ConnectorTable> connectorTables = new ArrayList<>();

            try {
                connectorTables = getTablesFrom3rdParty(catName, dbName, baseCanonicalName);
           //TODO correct exception type
            } catch (Exception e) {
                // throw Exception
                auditLog.logMessage(methodName, DbPollingOMRSAuditCode.GETTABLES_FAILED.getMessageDefinition(e.getMessage()));
            }
            return connectorTables;
        }

        /**
         * convert the connector tables to entities and relationships
         *
         * @param connectorTables connector tables
         * @throws ConnectorCheckedException connector exception
         * @throws TypeErrorException        type exception
         */
        void convertToConnectorTablesToEntitiesAndRelationships(List<ConnectorTable> connectorTables) throws ConnectorCheckedException, TypeErrorException {
            String methodName = "convertToConnectorTablesToEntitiesAndRelationships";

            for (ConnectorTable connectorTable : connectorTables) {
                String tableQualifiedName = connectorTable.getQualifiedName();
                EntityDetail tableEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                        SupportedTypes.TABLE,
                        connectorTable.getName(),
                        tableQualifiedName,
                        null,
                        true);


                tableEntity.setCreateTime(connectorTable.getCreateTime());


                List<Classification> tableClassifications = tableEntity.getClassifications();
                if (tableClassifications == null) {
                    tableClassifications = new ArrayList<>();
                }
                if (!sendEntitiesForSchemaType) {
                    Classification classification = mapperHelper.createTypeEmbeddedClassificationForTable(methodName, tableEntity);
                    tableClassifications.add(classification);
                }
                if ("VIRTUAL_VIEW".equals(connectorTable.getType())) {
                    //Indicate that this hmsTable is a view using the classification
                    tableClassifications.add(mapperHelper.createCalculatedValueClassification("refreshRepository", tableEntity, connectorTable.getHmsViewOriginalText()));
                }
                if (!tableClassifications.isEmpty()) {
                    tableEntity.setClassifications(tableClassifications);
                }

                saveEntityReferenceCopyForTable(tableEntity, tableQualifiedName);
                String tableGuid = tableEntity.getGUID();

                String tableTypeGUID = null; // only filled in when sendEntitiesForSchemaType = true
                if (sendEntitiesForSchemaType) {
                    // add schema type entity
                    EntityDetail tableEntityType = mapperHelper.getEntityDetailSkeleton(methodName,
                            SupportedTypes.RELATIONAL_TABLE_TYPE,
                            connectorTable.getName() + SupportedTypes.SEPARATOR_CHAR + "type",
                            connectorTable.getQualifiedName() + SupportedTypes.SEPARATOR_CHAR + SupportedTypes.SEPARATOR_CHAR + "type", // double separator to the type qualified name does not clash with an attribute qualified name
                            null,
                            true);

                    saveEntityReferenceCopyForTable(tableEntityType, tableQualifiedName);
                    tableTypeGUID = tableEntityType.getGUID();
                    Relationship relationship =mapperHelper.createReferenceRelationship(SupportedTypes.SCHEMA_ATTRIBUTE_TYPE,
                            tableEntity.getGUID(),
                            SupportedTypes.TABLE,
                            tableTypeGUID,
                            SupportedTypes.RELATIONAL_TABLE_TYPE);
                    saveRelationshipReferenceCopyForTable(relationship,tableQualifiedName);
                }

                // relationship


                Relationship relationship =mapperHelper.createReferenceRelationship(SupportedTypes.ATTRIBUTE_FOR_SCHEMA,
                        relationalDBTypeGuid,
                        SupportedTypes.RELATIONAL_DB_SCHEMA_TYPE,
                        tableGuid,
                        SupportedTypes.TABLE);
                saveRelationshipReferenceCopyForTable(relationship,tableQualifiedName);

                Iterator<ConnectorColumn> colsIterator = connectorTable.getColumns().listIterator();

                while (colsIterator.hasNext()) {
                    ConnectorColumn connectorColumn = colsIterator.next();
                    String columnName = connectorColumn.getName();
                    // TODO change the name for a derived column?

                    EntityDetail columnEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                            SupportedTypes.COLUMN,
                            columnName,
                            connectorTable.getQualifiedName() + SupportedTypes.SEPARATOR_CHAR + columnName,
                            null,
                            true);
                    String dataType = connectorColumn.getType();
                    if (!sendEntitiesForSchemaType) {
                        List<Classification> columnClassifications = columnEntity.getClassifications();
                        if (columnClassifications == null) {
                            columnClassifications = new ArrayList<>();
                        }

                        columnClassifications.add(mapperHelper.createTypeEmbeddedClassificationForColumn("refreshRepository", columnEntity, dataType));

                        columnEntity.setClassifications(columnClassifications);
                    }
                    saveEntityReferenceCopyForTable(columnEntity, tableQualifiedName);
                    if (sendEntitiesForSchemaType) {
                        // add schema type entity
                        EntityDetail columnEntityType = mapperHelper.getEntityDetailSkeleton(methodName,
                                SupportedTypes.RELATIONAL_COLUMN_TYPE,
                                columnName + SupportedTypes.SEPARATOR_CHAR + "type",
                                connectorTable.getQualifiedName() + SupportedTypes.SEPARATOR_CHAR + columnName + SupportedTypes.SEPARATOR_CHAR + "(type)",
                                null,
                                true);
                        if (dataType != null) {
                            InstanceProperties instanceProperties = columnEntityType.getProperties();
                            repositoryHelper.addStringPropertyToInstance(methodName, instanceProperties, "dataType", dataType, methodName);
                            columnEntityType.setProperties(instanceProperties);
                        }

                        saveEntityReferenceCopyForTable(columnEntityType, tableQualifiedName);
                        // create column to column type relationship
                         relationship = mapperHelper.createReferenceRelationship(SupportedTypes.SCHEMA_ATTRIBUTE_TYPE,
                                columnEntity.getGUID(),
                                SupportedTypes.COLUMN,
                                columnEntityType.getGUID(),
                                SupportedTypes.RELATIONAL_COLUMN_TYPE);
                        saveRelationshipReferenceCopyForTable(relationship,tableQualifiedName);

                        // create hmsTable type to column relationship
                        relationship = mapperHelper.createReferenceRelationship(SupportedTypes.ATTRIBUTE_FOR_SCHEMA,
                                tableTypeGUID,
                                SupportedTypes.RELATIONAL_TABLE_TYPE,
                                columnEntity.getGUID(),
                                SupportedTypes.COLUMN);
                        saveRelationshipReferenceCopyForTable(relationship,tableQualifiedName);

                    } else {
                        // relate the column to the table
                        relationship=mapperHelper.createReferenceRelationship(SupportedTypes.NESTED_SCHEMA_ATTRIBUTE,
                                tableGuid,
                                SupportedTypes.TABLE,
                                columnEntity.getGUID(),
                                SupportedTypes.COLUMN);
                        saveRelationshipReferenceCopyForTable(relationship,tableQualifiedName);
                    }
                }
            }
        }

        /**
         * Create Connection orientated entities. an Asset can be associated with a Connection,
         * which in turn has a ConnectionType and an Endpoint. Entities of these 3 types are created in this method
         * and relationships are created between them.
         * <p>
         * For more information on these entities see https://egeria-project.org/patterns/metadata-manager/overview/?h=asset+connections#asset-connections
         *
         * @param baseCanonicalName a unique name used as a base to create unique names for the entities
         * @param databaseEntity    the Database (which is a type of Asset)
         * @throws ConnectorCheckedException connector exception
         */
        private void createConnectionOrientatedEntities(String baseCanonicalName, EntityDetail databaseEntity) throws ConnectorCheckedException {
            String methodName = "createConnectionOrientatedEntities";
            String name = baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + "connection";
            String canonicalName = baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + "connection";

            EntityDetail connectionEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                    SupportedTypes.CONNECTION,
                    name,
                    canonicalName,
                    null,
                    false);

            saveEntityReferenceCopy(connectionEntity);

            name = baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + SupportedTypes.CONNECTOR_TYPE;
            canonicalName = baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + SupportedTypes.CONNECTOR_TYPE;

            EntityDetail connectionTypeEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                    SupportedTypes.CONNECTOR_TYPE,
                    name,
                    canonicalName,
                    null,
                    false);
            saveEntityReferenceCopy(connectionTypeEntity);


            name = baseCanonicalName + SupportedTypes.SEPARATOR_CHAR + SupportedTypes.ENDPOINT;
            canonicalName = name;

            EntityDetail endpointEntity = mapperHelper.getEntityDetailSkeleton(methodName,
                    SupportedTypes.ENDPOINT,
                    name,
                    canonicalName,
                    null,
                    false);
            InstanceProperties instanceProperties = endpointEntity.getProperties();
            //TODO does passing a protocol make sense here?
            if (configuredEndpointAddress != null) {
                repositoryHelper.addStringPropertyToInstance(methodName,
                        instanceProperties,
                        "networkAddress",
                        configuredEndpointAddress,
                        methodName);
                endpointEntity.setProperties(instanceProperties);
            }


            saveEntityReferenceCopy(endpointEntity);
            // create relationships

            // entity guids used to create proxies
            String connectionGuid = connectionEntity.getGUID();
            String dataFileGuid = databaseEntity.getGUID();
            String connectionTypeGuid = connectionTypeEntity.getGUID();
            String endPointGuid = endpointEntity.getGUID();

            // create the 3 relationships
            aboveTableRelationshipListToStore.add(mapperHelper.createReferenceRelationship(SupportedTypes.CONNECTION_TO_ASSET,
                    connectionGuid,
                    SupportedTypes.CONNECTION,
                    dataFileGuid,
                    SupportedTypes.DATABASE));

            aboveTableRelationshipListToStore.add(mapperHelper.createReferenceRelationship(SupportedTypes.CONNECTION_CONNECTOR_TYPE,
                    connectionGuid,
                    SupportedTypes.CONNECTION,
                    connectionTypeGuid,
                    SupportedTypes.CONNECTOR_TYPE));

            aboveTableRelationshipListToStore.add(mapperHelper.createReferenceRelationship(SupportedTypes.CONNECTION_ENDPOINT,
                    connectionGuid,
                    SupportedTypes.CONNECTION,
                    endPointGuid,
                    SupportedTypes.ENDPOINT
            ));
        }

        private void saveEntityReferenceCopy(EntityDetail entityToAdd) {
            aboveTableEntityListToStore.add(entityToAdd);
        }

        private void saveEntityReferenceCopyForTable(EntityDetail entityToAdd, String qualifiedTableName) {
            List<EntityDetail> entities = qualifiedTableNameToEntityMap.get(qualifiedTableName);
            if (entities == null) {
                entities= new ArrayList<>();
                entities.add(entityToAdd);
                qualifiedTableNameToEntityMap.put(qualifiedTableName,entities);
            } else {
                entities.add(entityToAdd);
            }
        }
        private void saveRelationshipReferenceCopyForTable(Relationship relationshipToAdd, String qualifiedTableName) {
            List<Relationship> relationships = qualifiedTableNameToRelationshipMap.get(qualifiedTableName);
            if (relationships ==null) {
                relationships =new ArrayList<>();
                relationships.add(relationshipToAdd);
                qualifiedTableNameToRelationshipMap.put(qualifiedTableName, relationships);
            } else {
                relationships.add(relationshipToAdd);
            }
        }
        /**
         * Issue the batch event with the list of supplied entities and relationships
         *
         * @param relationshipList relationships to include in the event
         * @param entityList       entities to include in the event
         */
        private void issueBatchEvent(List<Relationship> relationshipList, List<EntityDetail> entityList) {
            InstanceGraph instances = new InstanceGraph(entityList, relationshipList);

            // send the event
            repositoryEventProcessor.processInstanceBatchEvent("OMRSDatabasePollingRepositoryEventMapper",
                    repositoryConnector.getMetadataCollectionId(),
                    repositoryConnector.getServerName(),
                    repositoryConnector.getServerType(),
                    repositoryConnector.getOrganizationName(),
                    instances);
        }
    }

}
