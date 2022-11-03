/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.databasepollingrepository.helpers;

import org.odpi.egeria.connectors.databasepollingrepository.auditlog.DbPollingOMRSErrorCode;
import org.odpi.egeria.connectors.databasepollingrepository.helpers.ExceptionHelper;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefSummary;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class MapperHelper {
    private OMRSRepositoryHelper repositoryHelper          = null;
    private String userId = null;
    private String metadataCollectionId = null;

    private  String repositoryName = null;
    private String metadataCollectionName = null;
    private String qualifiedNamePrefix = null;

    public MapperHelper( OMRSRepositoryHelper repositoryHelper, String userId, String  metadataCollectionId, String repositoryName, String metadataCollectionName, String qualifiedNamePrefix) {
        this.repositoryHelper = repositoryHelper;
        this.userId = userId;
        this.metadataCollectionId=metadataCollectionId;
        this.repositoryName = repositoryName;
        this.metadataCollectionName = metadataCollectionName;
        this.qualifiedNamePrefix = qualifiedNamePrefix;

    }

    public Relationship createReferenceRelationship(String relationshipTypeName, String end1GUID, String end1TypeName, String end2GUID, String end2TypeName) throws ConnectorCheckedException {
        String methodName = "createReferenceRelationship";

        Relationship relationship = null;
        try {
            relationship = repositoryHelper.getSkeletonRelationship(methodName,
                    metadataCollectionId,
                    InstanceProvenanceType.LOCAL_COHORT,
                    userId,
                    relationshipTypeName);
            // leaving the version as 1 - until we have attributes we need to update
        } catch (TypeErrorException e) {
            ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.TYPE_ERROR_EXCEPTION, methodName, e);
        }

        String connectionToAssetCanonicalName = end1GUID + SupportedTypes.SEPARATOR_CHAR + relationshipTypeName + SupportedTypes.SEPARATOR_CHAR + end2GUID;
        String relationshipGUID = null;
        try {
            relationshipGUID = Base64.getUrlEncoder().encodeToString(connectionToAssetCanonicalName.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.ENCODING_EXCEPTION, methodName, e, "connectionToAssetCanonicalName", connectionToAssetCanonicalName);
        }

        relationship.setGUID(relationshipGUID);
        //end 1
        EntityProxy entityProxy1 = getEntityProxySkeleton(end1GUID, end1TypeName);
        relationship.setEntityOneProxy(entityProxy1);

        //end 2
        EntityProxy entityProxy2 = getEntityProxySkeleton(end2GUID, end2TypeName);
        relationship.setEntityTwoProxy(entityProxy2);


        return relationship;

    }

    /**
     * Create an entity proxy with the supplied parameters
     * @param guid GUID
     * @param typeName type name
     * @return entity proxy
     * @throws ConnectorCheckedException Connector errored
     */
    private EntityProxy getEntityProxySkeleton(String guid, String typeName) throws ConnectorCheckedException {
        String methodName = "getEntityProxySkeleton";
        EntityProxy proxy = new EntityProxy();
        TypeDefSummary typeDefSummary = repositoryHelper.getTypeDefByName("getEntityProxySkeleton", typeName);
        InstanceType type = null;
        try {
            if (typeDefSummary == null) {
                throw new TypeErrorException(DbPollingOMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN.getMessageDefinition(repositoryName, methodName, typeName),
                        this.getClass().getName(),
                        methodName);
            }
            type = repositoryHelper.getNewInstanceType(methodName, typeDefSummary);
        } catch (TypeErrorException e) {
            ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.TYPE_ERROR_EXCEPTION, methodName, e);
        }
        proxy.setType(type);
        proxy.setGUID(guid);
        proxy.setMetadataCollectionId(metadataCollectionId);
        proxy.setMetadataCollectionName(metadataCollectionName);
        return proxy;
    }

    /**
     * Create a skeleton of the entities, populated with the parameters supplied
     * @param originalMethodName callers method name - for diagnostics
     * @param typeName type name of the entity
     * @param name display name of the entity
     * @param canonicalName unique name
     * @param attributeMap map of attributes
     * @param generateUniqueVersion whether to generate a unique version (only required if we are going to update the entity)
     * @return EntityDetail created entity detail
     * @throws ConnectorCheckedException connector error
     */
    public EntityDetail getEntityDetailSkeleton(String originalMethodName,
                                                String typeName,
                                                String name,
                                                String canonicalName,
                                                Map<String, String> attributeMap,
                                                boolean generateUniqueVersion

    ) throws ConnectorCheckedException {
        String methodName = "getEntityDetail";

        String guid = null;
        try {
            guid = Base64.getUrlEncoder().encodeToString(canonicalName.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.ENCODING_EXCEPTION, methodName, e, "canonicalName", canonicalName);
        }


        InstanceProperties initialProperties = repositoryHelper.addStringPropertyToInstance(methodName,
                null,
                "name",
                name,
                methodName);
        initialProperties = repositoryHelper.addStringPropertyToInstance(methodName,
                initialProperties,
                "qualifiedName",
                qualifiedNamePrefix + canonicalName,
                methodName);
        if (attributeMap != null && !attributeMap.keySet().isEmpty()) {
            addPropertiesToInstanceProperties(initialProperties, attributeMap);
        }

        EntityDetail entityToAdd = new EntityDetail();
        entityToAdd.setProperties(initialProperties);

        // set the provenance as local cohort
        entityToAdd.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
        entityToAdd.setMetadataCollectionId(metadataCollectionId);

        TypeDef typeDef = repositoryHelper.getTypeDefByName(methodName, typeName);

        try {
            if (typeDef == null) {
                throw new TypeErrorException(DbPollingOMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN.getMessageDefinition(metadataCollectionName, methodName, typeName),
                        this.getClass().getName(),
                        originalMethodName);
            }
            InstanceType instanceType = repositoryHelper.getNewInstanceType(repositoryName, typeDef);
            entityToAdd.setType(instanceType);
        } catch (TypeErrorException e) {
            ExceptionHelper.raiseConnectorCheckedException(this.getClass().getName(), DbPollingOMRSErrorCode.TYPE_ERROR_EXCEPTION, methodName, e);
        }

        entityToAdd.setGUID(guid);
        entityToAdd.setStatus(InstanceStatus.ACTIVE);
        // for Entities that never change there is only a need for one version.
        // Entities never change if they have no attributes other than name - we generated the qualifiedName and GUID from
        // the name - so a change in name is a change in GUID, which would mean a delete then create.
        // For entities with properties then those properties could be updated and they require a version.
        long version = 1;
        if (generateUniqueVersion) {
            version = System.currentTimeMillis();
        }
        entityToAdd.setVersion(version);

        return entityToAdd;

    }

    /**
     * Add a map of properties to instance properties
     * @param properties instance properties to be updated
     * @param attributeMap map of properties
     */
    void addPropertiesToInstanceProperties(InstanceProperties properties, Map<String, String> attributeMap) {
        String methodName = "addPropertiesToInstanceProperties";
        if (attributeMap != null) {
            Set<Map.Entry<String,String>> entrySet = attributeMap.entrySet();
            Iterator<Map.Entry<String,String>> iter = entrySet.iterator();
            while (iter.hasNext()) {
                Map.Entry<String,String>  entry = iter.next();
                repositoryHelper.addStringPropertyToInstance(methodName,
                        properties,
                        entry.getKey(),
                        entry.getValue(),
                        methodName);
            }
        }
    }

    /**
     * Create the Calculated value classification
     * @param apiName api name for diagnostics
     * @param entity entity associated with the Classification
     * @param formula formula associated with the view
     * @return the Calculated Value classification
     * @throws TypeErrorException there is an error associated with the types
     */
    public Classification createCalculatedValueClassification(String apiName, EntityDetail entity, String formula) throws TypeErrorException {
        String methodName = "createCalculatedValueClassification";
        Classification classification = repositoryHelper.getSkeletonClassification(methodName, userId, SupportedTypes.CALCULATED_VALUE, entity.getType().getTypeDefName());
        InstanceProperties initialProperties = repositoryHelper.addStringPropertyToInstance(methodName,
                null,
                "formula",
                formula,
                methodName);
        classification.setProperties(initialProperties);
        repositoryHelper.addClassificationToEntity(apiName, entity, classification, methodName);

        return classification;
    }

    /**
     * Create embedded type classification for column
     * @param apiName api name for diagnostics
     * @param entity entity the classification is associated with
     * @param dataType type of the column
     * @return TypeEmbeddedClassification the type embedded classification
     * @throws TypeErrorException there is an error associated with the types
     */
    public Classification createTypeEmbeddedClassificationForColumn(String apiName, EntityDetail entity, String dataType) throws TypeErrorException {
        return createTypeEmbeddedClassification(apiName,SupportedTypes.RELATIONAL_COLUMN_TYPE, entity, dataType);
    }

    /**
     * Create embedded type classification for table
     *
     * @param apiName API name - for diagnostics
     * @param entity  - entity
     * @return the embedded type classification
     * @throws TypeErrorException there is an error associated with the types
     */
    public Classification createTypeEmbeddedClassificationForTable(String apiName, EntityDetail entity) throws TypeErrorException {

        return createTypeEmbeddedClassification(apiName, SupportedTypes.RELATIONAL_TABLE_TYPE, entity, null);
    }

    /**
     *
     * @param apiName API name for diagnostics
     * @param type the schema type name.
     * @param entity entity to apply the classification to
     * @param dataType column type if a column
     * @return the embedded type classification
     * @throws TypeErrorException there is an error associated with the types
     */
    private Classification createTypeEmbeddedClassification(String apiName, String type, EntityDetail entity, String dataType) throws TypeErrorException {
        String methodName = "createTypeEmbeddedClassification";
        Classification classification = repositoryHelper.getSkeletonClassification(methodName, userId, SupportedTypes.TYPE_EMBEDDED_ATTRIBUTE, entity.getType().getTypeDefName());
        InstanceProperties instanceProperties = new InstanceProperties();
        repositoryHelper.addStringPropertyToInstance(apiName, instanceProperties, "schemaTypeName", type, methodName);
        if (dataType != null ) {
            repositoryHelper.addStringPropertyToInstance(apiName, instanceProperties, "dataType", dataType, methodName);
        }
        classification.setProperties(instanceProperties);
        repositoryHelper.addClassificationToEntity(apiName, entity, classification, methodName);
        return classification;

    }

}
