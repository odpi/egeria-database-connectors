package org.odpi.openmetadata.adapters.connectors.integration.postgres;

/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
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

public class PostgresDatabaseConnector extends DatabaseIntegratorConnector
{

    @Override
    public void refresh() throws ConnectorCheckedException
    {
        String methodName = "PostgresConnector.refresh";
        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase(connectionProperties);

        try
        {
            List<PostgresDatabase> dbs = sourceDatabase.getDabaseNames();
 /*           List<DatabaseElement> knownDatabases = context.getMyDatabases(1, 100);
            if( knownDatabases != null )
            {
                for (DatabaseElement element : knownDatabases)
                {
                    String knownName = element.getDatabaseProperties().getQualifiedName();
                    for (PostgresDatabase db : dbs)
                    {
                        if (db.getQualifiedName().equals(knownName))
                        {
                            break;
                        }
                        context.removeDatabase(element.getElementHeader().getGUID(), knownName);
                    }
                }
            }
*/
            for (PostgresDatabase db : dbs)
            {

                List<DatabaseElement> database = this.context.getDatabasesByName(db.getQualifiedName(), 0, 100);
                if (database != null)
                {
                    updateDatabase( db );
                }
                else
                {
                    addDatabase(db);
                }
            }
        } catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (ConnectorCheckedException error)
        {
            // do nothing as it's already been handled
            throw error;
        } catch (Throwable error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    /**
     * Trawls through a database updating a database where necessary
     *
     * @param db              the bean properties of a Postgres Database
     * @throws ConnectorCheckedException
     */
    private void updateDatabase( PostgresDatabase db ) throws ConnectorCheckedException, InvalidParameterException, PropertyServerException, UserNotAuthorizedException, SQLException
    {
        String methodName = "updateDatabase";

        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
            /*
            first get a list of schemas for a given database
             */
            List<PostgresSchema> schemas = source.getDatabaseSchema(db.getQualifiedName());
            List<DatabaseElement> databases = context.getDatabasesByName(db.getQualifiedName(), 0, 100);

            String dbGuid = "";
            if( databases.size() > 0 )
            {
                dbGuid = databases.get(0).getElementHeader().getGUID();
            }
            else
            {
                //TODO log and ????
            }


            List<DatabaseSchemaElement> knownSchemas = context.getSchemasForDatabase(dbGuid, 0, 100);

            if( knownSchemas != null )
            {
                for (PostgresSchema sch : schemas)
                {
                    List<DatabaseSchemaElement> entity = context.getDatabaseSchemasByName(sch.getQualifiedName(), 1, 1);
                    if ( entity != null )
                    {
                        /*
                        new schema so add it and all it's child elements
                         */
                        addSchema(sch, dbGuid);
                    }
                    else
                    {
                        updateSchema(sch);
                    }

                }
            }

    }

    /**
     * Changes the properties of an egeria schema entity
     * @param sch the Postgres Schema properties
     *
     * @throws InvalidParameterException
     * @throws PropertyServerException
     * @throws UserNotAuthorizedException
     */
    private void updateSchema( PostgresSchema sch ) throws InvalidParameterException,
                                                           PropertyServerException,
                                                           UserNotAuthorizedException,
                                                           ConnectorCheckedException
    {
        /*
        find the original schema
         */
        List<DatabaseSchemaElement> schemas = this.context.getDatabaseSchemasByName(sch.getQualifiedName(), 0,100);
        DatabaseSchemaElement element = null;

        if( schemas != null )
        {
            element = schemas.get(0);
        }
        else
        {
            //TODO add logic for error condition of more than one
        }
        DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
        schemaProps.setDisplayName(sch.getSchema_name());
        schemaProps.setQualifiedName(sch.getQualifiedName());
        schemaProps.setOwner(sch.getSchema_owner());
        schemaProps.setAdditionalProperties(sch.getProperties());

        context.updateDatabaseSchema( element.getElementHeader().getGUID(), schemaProps);

        updateTablesForDatabaseSchema(sch, element);
    }

    /**
     *
     * @param sch
     * @param element
     * @throws InvalidParameterException
     * @throws PropertyServerException
     * @throws UserNotAuthorizedException
     */
    private void updateTablesForDatabaseSchema(PostgresSchema sch, DatabaseSchemaElement element) throws InvalidParameterException,
                                                                                                         PropertyServerException,
                                                                                                         UserNotAuthorizedException,
                                                                                                         ConnectorCheckedException
    {
        final String methodName = "updateTablesForDatabaseSchema";
        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase( this.connectionProperties);
        try
        {
            List<PostgresTable> tables = sourceDatabase.getTables(sch);
            List<DatabaseTableElement> knownTables = context.getTablesForDatabaseSchema(sch.getQualifiedName(), 1, 1000);

            for( DatabaseTableElement t : knownTables )
            {
                String knownName = t.getDatabaseTableProperties().getQualifiedName();
                for( PostgresTable table : tables )
                {
                    if( table.getQualifiedName().equals(knownName))
                    {
                        break;
                    }
                    /*
                    no longer hosted by the server, so remove
                     */
                    context.removeDatabaseTable( element.getElementHeader().getGUID(), knownName );
                }
            }

            for (PostgresTable t : tables)
            {
                List<DatabaseTableElement> tableElements = context.getDatabaseTablesByName(t.getQualifiedName(), 1, 1);
                if( tableElements.isEmpty() )
                {
                    addTable(t, element.getElementHeader().getGUID());
                }
                else
                {
                    updateTable(t);
                }


            }
        }
        catch (SQLException  error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    /**
     *
     * @param table
     * @throws InvalidParameterException
     * @throws PropertyServerException
     * @throws UserNotAuthorizedException
     */
    private void updateTable( PostgresTable table ) throws InvalidParameterException,
                                                           PropertyServerException,
                                                           UserNotAuthorizedException, ConnectorCheckedException
    {
        /*
        find the original schema
         */
        List<DatabaseTableElement> tables = this.context.getDatabaseTablesByName(table.getQualifiedName(), 1,1);
        DatabaseTableElement element = tables.get(0);

        DatabaseTableProperties props = new DatabaseTableProperties();
        props.setDisplayName(table.getTable_name() );
        props.setQualifiedName(table.getQualifiedName());
        props.setAdditionalProperties(table.getProperties());

        context.updateDatabaseTable( element.getElementHeader().getGUID(), props);

        updateColsForDatabaseTable(table);
    }

    private void updateColsForDatabaseTable(PostgresTable table) throws InvalidParameterException, PropertyServerException, UserNotAuthorizedException, ConnectorCheckedException
    {
        final String methodName = "updateColsForDatabaseTable";
        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
        try
        {
            List<PostgresColumn> columns = source.getColumnAttributes( table.getTable_name() );
            List<String> primaryKeys = source.getPrimaryKeyColumnNamesForTable(table.getTable_name());

            List<DatabaseColumnElement> knownColumns = context.getColumnsForDatabaseTable(table.getQualifiedName(), 1, 1000);
            for( DatabaseColumnElement c : knownColumns )
            {
                String knownName = c.getDatabaseColumnProperties().getQualifiedName();
                for( PostgresColumn col : columns )
                {
                    if( col.getQualifiedName().equals(knownName))
                    {
                        break;
                    }
                    /*
                    no longer hosted by the server, so remove
                     */
                    context.removeDatabaseColumn( c.getElementHeader().getGUID(), knownName );
                }
            }

            for (PostgresColumn col : columns )
            {
                List<DatabaseColumnElement> colElements = context.getDatabaseColumnsByName(col.getQualifiedName(), 1, 1);
                DatabaseColumnElement element = colElements.get(0);
                if( colElements.isEmpty() )
                {
                    addColumn(col, element.getElementHeader().getGUID(), primaryKeys);
                }
                else
                {
                    updateColumn(col);
                }


            }
        }
        catch (SQLException error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    private void updateColumn( PostgresColumn col) throws InvalidParameterException, PropertyServerException, UserNotAuthorizedException
    {
        /*
        find the original schema
         */
        List<DatabaseColumnElement> columns = this.context.getDatabaseColumnsByName(col.getQualifiedName(), 1,1);
        DatabaseColumnElement element = columns.get(0);

        DatabaseTableProperties props = new DatabaseTableProperties();
        props.setDisplayName(col.getTable_name() );
        props.setQualifiedName(col.getQualifiedName());
        props.setAdditionalProperties(col.getProperties());

        context.updateDatabaseTable( element.getElementHeader().getGUID(), props);

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db the postgres attributes of the database
     * @throws ConnectorCheckedException
     */
    private void addDatabase(PostgresDatabase db) throws ConnectorCheckedException
    {
        String methodName = "addDatabase";
        String guid = null;
        try
        {
         /*
         new database so build the database in egeria
         */
            DatabaseProperties dbProps = new DatabaseProperties();
            dbProps.setDisplayName(db.getName());
            dbProps.setQualifiedName(db.getQualifiedName());
/*            dbProps.setDatabaseType("postgres");     //TODO ??
            dbProps.setDatabaseVersion(db.getVersion());

            dbProps.setEncodingType(db.getEncoding());
            dbProps.setEncodingLanguage(db.getCtype());
            dbProps.setDatabaseImportedFrom( "https://localhost:5432");
            dbProps.setDatabaseInstance("localhost");
            dbProps.setDatabaseVersion("0.1");
            dbProps.setDescription("Postgres First Test Database ");
*/
            //TODO we need to clarify the source of the following properties
            /*
            ass Hostname
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
            dbProps.setAdditionalProperties(db.getProperties());

            guid = this.context.createDatabase(dbProps);
            addSchemasForDatabase(db, guid);

        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_DATABASES.getMessageDefinition(db.getName()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(db.getName()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(db.getName()),
                        error);
            }
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    private void addSchemasForDatabase(PostgresDatabase db, String currentDBGUID) throws ConnectorCheckedException
    {

        String methodName = "addSchemasForDatabase";

        try
        {
            PostgresSourceDatabase sourceDB = new PostgresSourceDatabase(this.connectionProperties);
            List<PostgresSchema> schemas = sourceDB.getDatabaseSchema(db.getName());

            if( schemas == null )
            {
                 if (this.auditLog != null)
                {
                    //auditLog.logMessage("addSchema", );
                }

            }
            for (PostgresSchema schema : schemas)
            {
                addSchema(schema, currentDBGUID);
            }

        } catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_SCHEMAS.getMessageDefinition(db.getName()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(db.getName()),
                        error);

            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);



        } catch (UserNotAuthorizedException error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(db.getName()),
                        error);

            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);



        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(db.getName()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sch     the postgres schema attributes to be
     * @param dbGuidd the egeria GUID of the database
     * @throws ConnectorCheckedException
     */
    private void addSchema(PostgresSchema sch, String dbGuidd) throws ConnectorCheckedException,
                                                                      InvalidParameterException,
                                                                      PropertyServerException,
                                                                      UserNotAuthorizedException
    {
        String methodName = "addSchema";

        PostgresSourceDatabase sourceDB = new PostgresSourceDatabase(this.connectionProperties);

        DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
        schemaProps.setDisplayName(sch.getSchema_name());
        schemaProps.setQualifiedName(sch.getQualifiedName());
        schemaProps.setOwner(sch.getSchema_owner());
        schemaProps.setAdditionalProperties(sch.getProperties());

        String schemaGUID = this.context.createDatabaseSchema(dbGuidd, schemaProps);
        addTablesForSchema(sourceDB, sch, schemaGUID);
        addViews(sourceDB, sch, schemaGUID);
        addForeignKeys(sourceDB, sch);

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
    private void addTablesForSchema(PostgresSourceDatabase sourceDB, PostgresSchema schema, String schemaGUID) throws ConnectorCheckedException
    {
        String methodName = "addTablesForSchema";
        List<PostgresTable> tables;

        try
        {
            /* add the schema tables */
            tables = sourceDB.getTables(schema);
            for (PostgresTable table : tables)
            {
                    addTable(table, schemaGUID);
            }
        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_TABLES.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (UserNotAuthorizedException error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (Throwable error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        }

    }


    private void addTable(PostgresTable table, String schemaGUID) throws InvalidParameterException,
                                                                         PropertyServerException,
                                                                         UserNotAuthorizedException,
                                                                         ConnectorCheckedException
    {
        DatabaseTableProperties props = new DatabaseTableProperties();
        props.setDisplayName(table.getTable_name());
        props.setQualifiedName(table.getQualifiedName());
        props.setAdditionalProperties( table.getProperties() );
        String tableGUID = this.context.createDatabaseTable(schemaGUID, props);
        addColumnsForTable(table, tableGUID);
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sourceDB the source postgres database
     * @param schema   the attributes of the schema which owns the tables
     * @throws ConnectorCheckedException
     */
    private void addForeignKeys(PostgresSourceDatabase sourceDB, PostgresSchema schema) throws ConnectorCheckedException
    {
        String methodName = "addForeignKeys";
        try
        {

            List<PostgresTable> tables = sourceDB.getTables(schema);

            for (PostgresTable t : tables)
            {
                List<PostgresForeginKeyLinks> foreginKeys = sourceDB.getForeginKeyLinksForTable(t.getTable_name());
                List<String> importedGuids = new ArrayList<>();
                List<String> exportedGuids = new ArrayList<>();

                for (PostgresForeginKeyLinks link : foreginKeys)
                {
                    List<DatabaseColumnElement> importedEntities = this.context.findDatabaseColumns(link.getImportedColumnQualifiedName(), 1, 20);
                    for (DatabaseColumnElement col : importedEntities)
                    {
                        importedGuids.add(col.getReferencedColumnGUID());
                    }

                    List<DatabaseColumnElement> exportedEntities = this.context.findDatabaseColumns(link.getExportedColumnQualifiedName(), 1, 1);

                    for (DatabaseColumnElement col : exportedEntities)
                    {
                        exportedGuids.add(col.getReferencedColumnGUID());
                    }

                    DatabaseForeignKeyProperties linkProps = new DatabaseForeignKeyProperties();
                    this.context.addForeignKeyRelationship(importedGuids.get(0), exportedGuids.get(0), linkProps);

                }
            }
        } catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_FOREGIN_KEYS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (PropertyServerException error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (Throwable error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

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
    private void addViews(PostgresSourceDatabase sourceDB, PostgresSchema schema, String schemaGUID) throws ConnectorCheckedException
    {
        String methodName = "addViews";

        try
        {
            List<PostgresTable> views = sourceDB.getViews(schema);

            for (PostgresTable view : views)
            {
                DatabaseViewProperties viewProps = new DatabaseViewProperties();
                viewProps.setDisplayName(view.getTable_name());
                viewProps.setQualifiedName(view.getQualifiedName());
                viewProps.setAdditionalProperties( view.getProperties());

                String tableGUID = this.context.createDatabaseView(schemaGUID, viewProps);

                /* TODO add links to exisitng columns ??*/
                addColumnsForTable( view, tableGUID );
            }


        } catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (SQLException error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_VIEWS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        } catch (UserNotAuthorizedException error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(),
                    this.getClass().getName(),
                    methodName);

        } catch (ConnectorCheckedException error)
        {
            // already handled , so just propogate up
            throw error;
        } catch (Throwable error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param table     the attributes of the view
     * @param tableGUID the GUID of the owning table
     * @throws ConnectorCheckedException thrown by the JDBC Driver
     */
    private void addColumnsForTable(PostgresTable table, String tableGUID) throws ConnectorCheckedException
    {
        String methodName = "addColumnsForTable";

        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
        try
        {
            List<String> primaryKeys = source.getPrimaryKeyColumnNamesForTable(table.getTable_name());
            List<PostgresColumn> cols = source.getColumnAttributes(table.getTable_name());

            for (PostgresColumn col : cols)
            {
                DatabaseColumnProperties colProps = new DatabaseColumnProperties();
                colProps.setDisplayName(col.getColumn_name());
                colProps.setQualifiedName(col.getQualifiedName());
                colProps.setAdditionalProperties(col.getProperties());

                String colGUID = this.context.createDatabaseColumn(tableGUID, colProps);

                if (primaryKeys.contains(col.getColumn_name()))
                {
                    DatabasePrimaryKeyProperties keyProps = new DatabasePrimaryKeyProperties();
                    keyProps.setName(col.getColumn_name());
                    this.context.setPrimaryKeyOnColumn(colGUID, keyProps);
                }
            }
        } catch (InvalidParameterException e)
        {
            e.printStackTrace();
        } catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_COLUMNS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        } catch (Throwable error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    private void addColumn( PostgresColumn col, String guid, List<String> primaryKeys ) throws InvalidParameterException, PropertyServerException, UserNotAuthorizedException
    {
        DatabaseColumnProperties colProps = new DatabaseColumnProperties();
        colProps.setDisplayName(col.getColumn_name());
        colProps.setQualifiedName(col.getQualifiedName());
        colProps.setAdditionalProperties(col.getProperties());

        String colGUID = this.context.createDatabaseColumn(guid, colProps);

        if (primaryKeys.contains(col.getColumn_name()))
        {
            DatabasePrimaryKeyProperties keyProps = new DatabasePrimaryKeyProperties();
            keyProps.setName(col.getColumn_name());
            this.context.setPrimaryKeyOnColumn(colGUID, keyProps);
        }

    }
}


