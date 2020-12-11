package org.odpi.openmetadata.adapters.connectors.integration.postgres;

/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.*;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.PostgresConnectorAuditCode;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.PostgresConnectorErrorCode;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresColumn;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresForeginKeyLinks;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresSchema;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresTable;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresDatabaseConnector extends DatabaseIntegratorConnector {

    @Override
    public void refresh ( ) throws ConnectorCheckedException {
        String methodName = "PostgresConnector.refresh";

        //TODO This all needs to be re written so as the controllling loop is through Egeria entities and we then HAVE to as ask postgres if the known state has changed
        //TODO or is it ok to as for exact database searches such as the below , where only a single entity will be returned

        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase(connectionProperties);

        try {
            List <PostgresDatabase> dbs = sourceDatabase.getDabaseNames ( );

            for ( PostgresDatabase db : dbs ) {

                List < DatabaseElement > database = this.context.findDatabases ( db.getQualifiedName ( ) , 1 , 1 );

                if ( ! database.isEmpty ( ) ) {

                } else {
                    /*
                     */
                    addDatabase ( db );
                }
            }
        } catch ( SQLException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( UserNotAuthorizedException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( ConnectorCheckedException error ) {
            // do nothing as it's already been handled
            throw error;
        } catch ( Throwable error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        }

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db the postgres attributes of the database
     * @throws ConnectorCheckedException
     */
    private void addDatabase ( PostgresDatabase db ) throws ConnectorCheckedException {
        String methodName = "addDatabase";
        String currentDBGUID = null;
        try {
         /*
         new database so build the database in egeria
         */
            DatabaseProperties dbProps = new DatabaseProperties ( );
            dbProps.setDisplayName ( db.getName ( ) );
            dbProps.setQualifiedName ( db.getQualifiedName ( ) );
            dbProps.setDatabaseType ( "postgres" );     //TODO ??
            dbProps.setDatabaseInstance(db.getInstance());  //TODO usr@server_addr@port
            dbProps.setDatabaseVersion ( db.getVersion ( ) );
            dbProps.setEncodingType ( db.getEncoding ( ) );
            dbProps.setEncodingLanguage ( db.getCtype ( ) );
            dbProps.setOwner( db.getOwner() );     //TODO The owner of the database or the owner of the metadata


            //TODO we need to clarify the source of the following properties
            /*
                 "DatabaseProperties{" +
                        "databaseType='" + databaseType + '\'' +
                        ", databaseImportedFrom='" + databaseImportedFrom + '\'' +
                        ", createTime=" + getCreateTime() +
                        ", modifiedTime=" + getModifiedTime() +
                        ", encodingDescription='" + getEncodingDescription() + '\'' +
                        ", owner='" + getOwner() + '\'' +
                        ", ownerCategory=" + getOwnerCategory() +
                        ", zoneMembership=" + getZoneMembership() +
                        ", origin=" + getOtherOriginValues() +
                        ", typeName='" + getTypeName() + '\'' +
                        ", extendedProperties=" + getExtendedProperties() +
                        '}';
}
             */



            /* just to aid dev/debug , there are currently no plans to add any AdditionalProperties */
            dbProps.setAdditionalProperties ( db.getProperties ( ) );


            currentDBGUID = this.context.createDatabase ( dbProps );
            addSchemas ( db , currentDBGUID );

        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition ( db.getName ( ) ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition ( db.getName ( ) ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( UserNotAuthorizedException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( db.getName ( ) ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( db.getName ( ) ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( db.getName ( ) ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( db.getName ( ) ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        }

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db      the postgres attributes of the database
     * @param dbGuidd the egeria GUID of the database
     * @throws ConnectorCheckedException
     */
    private void addSchemas ( PostgresDatabase db , String dbGuidd ) throws ConnectorCheckedException {
        String methodName = "addSchemas";
        try {

            PostgresSourceDatabase sourceDB = new PostgresSourceDatabase ( this.connectionProperties );
            List <PostgresSchema> schemas = sourceDB.getDatabaseSchema( db.getName ( ) );

            for ( PostgresSchema schema : schemas ) {
                DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties ( );
                schemaProps.setDisplayName ( schema.getSchema_name ( ) );
                schemaProps.setQualifiedName ( schema.getQualifiedName ( ) );
                String schemaGUID = this.context.createDatabaseSchema ( dbGuidd , schemaProps );
                /*insert all tables views and cols*/
                addTablesForSchema ( sourceDB , schema , schemaGUID );
                addViews ( sourceDB , schema , schemaGUID );
                addForeignKeys ( sourceDB , schema );
            }
        } catch ( SQLException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_SCHEMAS.getMessageDefinition ( db.getName ( ) ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_SCHEMAS.getMessageDefinition ( db.getName ( ) ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition ( db.getName ( ) ) ,
                        error );

            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );


        } catch ( UserNotAuthorizedException error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( db.getName ( ) ) ,
                        error );

            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );


        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( db.getName ( ) ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        }

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sourceDB   the source postgres database
     * @param schema     the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws ConnectorCheckedException
     */
    private void addTablesForSchema ( PostgresSourceDatabase sourceDB , PostgresSchema schema , String schemaGUID ) throws ConnectorCheckedException {
        String methodName = "addTablesForSchema";
        List <PostgresTable> tables;

        try {
            /* add the schema tables */
            tables = sourceDB.getTables ( schema );

            for ( PostgresTable table : tables ) {

                DatabaseTableProperties tableProps = new DatabaseTableProperties ( );
                tableProps.setDisplayName ( table.getTable_name ( ) );
                tableProps.setQualifiedName ( table.getQualifiedName ( ) );
                String tableGUID = this.context.createDatabaseTable ( schemaGUID , tableProps );
                addColumnsForTable(sourceDB, table, tableGUID);

            }
        } catch ( SQLException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_TABLES.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_TABLES.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( UserNotAuthorizedException error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( Throwable error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        }

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sourceDB   the source postgres database
     * @param schema     the attributes of the schema which owns the tables
     * @throws ConnectorCheckedException
     */
    private void addForeignKeys ( PostgresSourceDatabase sourceDB , PostgresSchema schema  ) throws ConnectorCheckedException {
        String methodName = "addForeignKeys";

        try {

            List < PostgresTable > tables = sourceDB.getTables ( schema );

            for ( PostgresTable t : tables ) {
                List <PostgresForeginKeyLinks> foreginKeys = sourceDB.getForeginKeyLinksForTable ( t.getTable_name ( ) );
                List < String > importedGuids = new ArrayList <> ( );
                List < String > exportedGuids = new ArrayList <> ( );

                for ( PostgresForeginKeyLinks link : foreginKeys ) {
                    List < DatabaseColumnElement > importedEntities = this.context.findDatabaseColumns ( link.getImportedColumnQualifiedName ( ) , 1 , 20 );
                    for ( DatabaseColumnElement col : importedEntities ) {
                        importedGuids.add ( col.getReferencedColumnGUID ( ) );
                    }

                    List < DatabaseColumnElement > exportedEntities = this.context.findDatabaseColumns ( link.getExportedColumnQualifiedName ( ) , 1 , 1 );

                    for ( DatabaseColumnElement col : exportedEntities ) {
                        exportedGuids.add ( col.getReferencedColumnGUID ( ) );
                    }

                    DatabaseForeignKeyProperties linkProps = new DatabaseForeignKeyProperties ( );
                    this.context.addForeignKeyRelationship ( importedGuids.get ( 0 ) , exportedGuids.get ( 0 ) , linkProps );

                }
            }
        } catch ( SQLException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_FOREGIN_KEYS.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_FOREIGN_KEYS.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( UserNotAuthorizedException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( PropertyServerException error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( Throwable error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );


        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sourceDB   the source postgres database
     * @param schema     the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws ConnectorCheckedException thrown by the JDBC Driver
     */
    private void addViews ( PostgresSourceDatabase sourceDB , PostgresSchema schema , String schemaGUID ) throws ConnectorCheckedException {
        String methodName = "addViews";

        try {
            List < PostgresTable > views = sourceDB.getViews ( schema );

            for ( PostgresTable view : views ) {
                DatabaseViewProperties viewProps = new DatabaseViewProperties ( );
                viewProps.setDisplayName ( view.getTable_name ( ) );
                viewProps.setQualifiedName ( view.getQualifiedName ( ) );
                String tableGUID = this.context.createDatabaseView ( schemaGUID , viewProps );

                /* TODO add links to exisitng columns ??*/
                addColumnsForTable ( sourceDB , view , tableGUID );
            }


        } catch ( InvalidParameterException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( SQLException error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_VIEWS.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_VIEWS.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( UserNotAuthorizedException error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( ConnectorCheckedException error ) {
            // already handled , so just propogate up
            throw error;
        } catch ( Throwable error ) {

            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sourceDB  the source postgres database
     * @param table     the attributes of the view
     * @param tableGUID the GUID of the owning table
     * @throws ConnectorCheckedException thrown by the JDBC Driver
     */
    private void addColumnsForTable ( PostgresSourceDatabase sourceDB , PostgresTable table , String tableGUID ) throws ConnectorCheckedException {
        String methodName = "addColumnsForTable";

        try {
            List < String > primaryKeys = sourceDB.getPrimaryKeyColumnNamesForTable ( table.getTable_name ( ) );
            List <PostgresColumn> cols = sourceDB.getColumnAttributes ( table.getTable_name ( ) );

            for ( PostgresColumn col : cols ) {
                DatabaseColumnProperties colProps = new DatabaseColumnProperties ( );
                colProps.setDisplayName ( col.getColumn_name ( ) );
                colProps.setQualifiedName ( col.getQualifiedName ( ) );
                colProps.setAdditionalProperties ( col.getProperties ( ) );

                String colGUID = this.context.createDatabaseColumn ( tableGUID , colProps );

                if ( primaryKeys.contains ( col.getColumn_name ( ) ) ) {

                    DatabasePrimaryKeyProperties keyProps = new DatabasePrimaryKeyProperties ( );

                    keyProps.setName ( col.getColumn_name ( ) );
                    this.context.setPrimaryKeyOnColumn ( colGUID , keyProps );
                }
            }
        } catch ( InvalidParameterException e ) {
            e.printStackTrace ( );
        } catch ( SQLException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.ERROR_READING_COLUMNS.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.ERROR_READING_COLUMNS.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( PropertyServerException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.INVALID_PROPERTY.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );
        } catch ( UserNotAuthorizedException error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        } catch ( Throwable error ) {
            if ( this.auditLog != null ) {
                auditLog.logException ( methodName ,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition ( ) ,
                        error );
            }

            throw new ConnectorCheckedException ( PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition ( ) ,
                    this.getClass ( ).getName ( ) ,
                    methodName );

        }

    }
}


