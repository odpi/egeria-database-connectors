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
import org.odpi.openmetadata.adapters.connectors.integration.postgres.mapper.PostgresMapper;
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
        int startFrom = 1;
        int pageSize = 100;

        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase(connectionProperties);
        try
        {
            /*
            get a list of databases currently hosted in postgres
            and remove any databases that have been removed since the last refresh
             */
            List<PostgresDatabase> dbs = sourceDatabase.getDabaseNames();
            List <DatabaseElement> knownDatabases = getContext().getMyDatabases( startFrom, pageSize);

            for (PostgresDatabase db : dbs)
            {
                boolean found = false;

                for( DatabaseElement base : knownDatabases )
                {
                    if( base.getDatabaseProperties().getQualifiedName().equals( db.getQualifiedName()))
                    {
                        /*
                        we have found an exact instance to update
                         */
                        found = true;
                        updateDatabase(db, knownDatabases );
                        break;
                    }
                }
                    /*
                    this is a new table so add it
                     */
                if( found == false )
                {
                    addDatabase( db );
                }
            }
        }
        catch (SQLException error)
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
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    /**
     * Checks if any databases need to be removed from egeria
     *
     * @param dbs              a list of the bean properties of a Postgres Database
     * @param knownDatabases    a list of the Databases already known to egeria
     * @throws                  ConnectorCheckedException
     */
    private void purgeDatabases(List<PostgresDatabase> dbs, List<DatabaseElement> knownDatabases) throws ConnectorCheckedException
    {

        String methodName = "purgeDatabases";
        int startFrom = 0;
        int pageSize = 100;

        try
        {
            if (knownDatabases != null)
            {
                for (DatabaseElement element : knownDatabases)
                {
                    String knownName = element.getDatabaseProperties().getQualifiedName();
                    for (PostgresDatabase db : dbs)
                    {
                        String sourceName = db.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            break;
                        }
                        getContext().removeDatabase(element.getElementHeader().getGUID(), knownName);
                    }
                }
            }
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
    }

    /**
     * Trawls through a database updating a database where necessary
     *
     * @param db              the bean properties of a Postgres Database
     * @param databases
     * @throws ConnectorCheckedException
     */
    private void updateDatabase(PostgresDatabase db, List<DatabaseElement> databases) throws ConnectorCheckedException, InvalidParameterException, PropertyServerException, UserNotAuthorizedException, SQLException
    {
        String methodName = "updateDatabase";
        int startFrom = 1;
        int pageSize = 100;

        try
        {
            PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
            List<PostgresSchema> schemas = source.getDatabaseSchema(db.getQualifiedName());

            if( databases != null )
            {
                for ( DatabaseElement element : databases )
                {
                    String dbGuid = element.getElementHeader().getGUID();
                    List<DatabaseSchemaElement> knownSchemas = getContext().getSchemasForDatabase(dbGuid, startFrom, pageSize);
                    if( knownSchemas != null )
                    {
                        for (PostgresSchema sch : schemas)
                        {
                            List<DatabaseSchemaElement> entity = getContext().getDatabaseSchemasByName(sch.getQualifiedName(), startFrom, pageSize);
                            if ( entity != null )
                            {
                                addSchema(sch, dbGuid);
                            }
                            else
                            {
                                updateSchema(sch, dbGuid);
                            }

                        }
                    }
                }

            }
        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_SCHEMAS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_SCHEMAS.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }

    }

    /**
     * Changes the properties of an egeria schema entity
     * @param sch the Postgres Schema properties
     *
     * @param dbGuid
     * @throws InvalidParameterException
     * @throws PropertyServerException
     * @throws UserNotAuthorizedException
     */
    private void updateSchema(PostgresSchema sch, String dbGuid) throws ConnectorCheckedException
    {
        String methodName = "updateSchema";
        int startFrom = 1;
        int pageSize = 100;
        try
        {
            List<DatabaseSchemaElement> schemas = getContext().getSchemasForDatabase(dbGuid, startFrom, pageSize);
            for (DatabaseSchemaElement egSch : schemas)
            {
                DatabaseSchemaProperties schemaProps = new DatabaseSchemaProperties();
                schemaProps.setDisplayName(sch.getQualifiedName());
                schemaProps.setQualifiedName(sch.getQualifiedName());
                schemaProps.setOwner(sch.getSchema_owner());
                schemaProps.setAdditionalProperties(sch.getProperties());
                getContext().updateDatabaseSchema(egSch.getElementHeader().getGUID(), schemaProps);
                updateTables(sch, egSch);
            }
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
    }

    /**
     *
     * @param sch           the postgres schema bean
     * @param schema        the egeria schema bean
     * @throws ConnectorCheckedException
     */
    private void updateTables(PostgresSchema sch, DatabaseSchemaElement schema) throws ConnectorCheckedException
    {
        final String methodName = "updateTablesForDatabaseSchema";
        int startFrom = 1;
        int pageSize = 100;

        String schemaGuid = schema.getElementHeader().getGUID();
        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase( this.connectionProperties);

        try
        {
            List<PostgresTable> tables = sourceDatabase.getTables(sch);
            List<DatabaseTableElement> knownTables = getContext().getTablesForDatabaseSchema(schemaGuid, startFrom, pageSize);

            if (knownTables != null)
            {
                for (DatabaseTableElement t : knownTables)
                {
                    String knownName = t.getDatabaseTableProperties().getQualifiedName();
                    for (PostgresTable table : tables)
                    {
                        if (table.getQualifiedName().equals(knownName))
                        {
                            break;
                        }
                        getContext().removeDatabaseTable(schemaGuid, knownName);
                    }
                }

                for (PostgresTable t : tables)
                {
                    boolean found = false;
                    for( DatabaseTableElement table : knownTables )
                    {
                            if( table.getDatabaseTableProperties().getQualifiedName().equals( t.getQualifiedName()))
                            {
                                /*
                                we have found an exact instance to update
                                 */
                                updateTable(t, schemaGuid );
                                found = true;
                                break;
                            }
                    }
                    /*
                    this is a new table so add it
                     */
                    if( found == false )
                    {
                        addTable( t, schemaGuid);
                    }
                }
            }

        }
        catch (SQLException  error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_TABLES.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_TABLES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    /**
     *
     * @param table
     * @param schemaGuid
     * @throws InvalidParameterException
     * @throws PropertyServerException
     * @throws UserNotAuthorizedException
     */
    private void updateTable(PostgresTable table, String schemaGuid) throws ConnectorCheckedException
    {
        String methodName = "updateTable";
        int startFrom = 1;
        int pageSize = 100;

        try
        {
                DatabaseTableProperties props = new DatabaseTableProperties();
                props.setDisplayName(table.getQualifiedName());
                props.setQualifiedName(table.getQualifiedName());
                props.setAdditionalProperties(table.getProperties());
                getContext().updateDatabaseTable(schemaGuid, props);
                updateCols(table);
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    private void updateCols(PostgresTable table) throws InvalidParameterException, PropertyServerException, UserNotAuthorizedException, ConnectorCheckedException
    {
        final String methodName = "updateCols";
        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
        try
        {
            List<PostgresColumn> columns = source.getColumnAttributes( table.getTable_name() );
            List<String> primaryKeys = source.getPrimaryKeyColumnNamesForTable(table.getTable_name());

            List<DatabaseColumnElement> knownColumns = getContext().getColumnsForDatabaseTable(table.getQualifiedName(), 1, 1000);
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
                    getContext().removeDatabaseColumn( c.getElementHeader().getGUID(), knownName );
                }
            }

            for (PostgresColumn col : columns )
            {
                List<DatabaseColumnElement> colElements = getContext().getDatabaseColumnsByName(col.getQualifiedName(), 1, 1);
                DatabaseColumnElement element = colElements.get(0);
                if( colElements != null)
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
                        PostgresConnectorAuditCode.ERROR_READING_COLUMNS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_COLUMNS.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    private void updateColumn( PostgresColumn col) throws ConnectorCheckedException
    {
        String methodName = "updateColumn";
        int stratFrom = 1;
        int pageSize = 100;

        try
        {
            List<DatabaseColumnElement> columns = this.getContext().getDatabaseColumnsByName(col.getQualifiedName(), stratFrom, pageSize);
            DatabaseColumnElement element = columns.get(0);

            DatabaseTableProperties props = new DatabaseTableProperties();
            props.setDisplayName(col.getTable_name());
            props.setQualifiedName(col.getQualifiedName());
            props.setAdditionalProperties(col.getProperties());
            getContext().updateDatabaseTable(element.getElementHeader().getGUID(), props);
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

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
            PostgresMapper mapper  = new PostgresMapper();
            DatabaseProperties dbProps = mapper.mapDatabaseProperties(db);
            guid = this.getContext().createDatabase(dbProps);
            addSchemas(db.getName(), guid);

        }
        catch (InvalidParameterException error)
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

    /**
     * Adds schema entites to egeria for a given database
     *
     * @param dbName the name of the database
     * @param dbGUID the GUID of the datbase enitity to attach the schemas
     * @throws ConnectorCheckedException
     */
    private void addSchemas(String dbName, String dbGUID) throws ConnectorCheckedException
    {

        String methodName = "addSchemas";
        try
        {
            PostgresSourceDatabase sourceDB = new PostgresSourceDatabase(this.connectionProperties);
            List<PostgresSchema> schemas = sourceDB.getDatabaseSchema( dbName );
            for ( PostgresSchema sch : schemas )
            {
                addSchema(sch, dbGUID);
            }

        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_SCHEMAS.getMessageDefinition(dbName),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_SCHEMAS.getMessageDefinition(error.getClass().getName(),
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
    private void addSchema(PostgresSchema sch, String dbGuidd) throws ConnectorCheckedException
    {
        String methodName = "addSchema";
        try
        {
            PostgresSourceDatabase sourceDB = new PostgresSourceDatabase(this.connectionProperties);
            PostgresMapper mapper = new PostgresMapper();
            DatabaseSchemaProperties schemaProps = mapper.mapSchemaProlperties(sch);
            String schemaGUID = getContext().createDatabaseSchema(dbGuidd, schemaProps);
            addTables(sourceDB, sch, schemaGUID);
            addViews(sourceDB, sch, schemaGUID);
            addForeignKeys(sourceDB, sch);
        }
        catch ( UserNotAuthorizedException error)
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


        }
        catch ( PropertyServerException error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_DATABASES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch ( InvalidParameterException error )
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
        }
        catch (Exception error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
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
     * @throws ConnectorCheckedException
     */
    private void addTables(PostgresSourceDatabase sourceDB, PostgresSchema schema, String schemaGUID) throws
                                                                                                               ConnectorCheckedException
    {
        String methodName = "addTables";
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

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_TABLES.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        }

    }

    /**
     * Reads the postgres metadata for a given table and creates a corresponding egeria entity.
     *
     * @param table     the postgres schema attributes to be
     * @param schemaGUID the egeria GUID of the schema
     * @throws ConnectorCheckedException
     */
    private void addTable(PostgresTable table, String schemaGUID) throws ConnectorCheckedException
    {
        String methodName = "addTable";
        try
        {
            DatabaseTableProperties props = new DatabaseTableProperties();
            props.setDisplayName(table.getQualifiedName());
            props.setQualifiedName(table.getQualifiedName());
            props.setAdditionalProperties(table.getProperties());
            String tableGUID = this.getContext().createDatabaseTable(schemaGUID, props);
            addColumns(table, tableGUID);
        }
        catch ( ConnectorCheckedException error )
        {

        }
        catch ( UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch ( InvalidParameterException error )
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);


        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

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
        int startFrom = 1;
        int pageSize = 100;

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
                    List<DatabaseColumnElement> importedEntities = this.getContext().findDatabaseColumns(link.getImportedColumnQualifiedName(), startFrom, pageSize) ;
                    if( importedEntities != null )
                    {
                        for (DatabaseColumnElement col : importedEntities)
                        {
                            importedGuids.add(col.getReferencedColumnGUID());
                        }
                    }

                    List<DatabaseColumnElement> exportedEntities = this.getContext().findDatabaseColumns(link.getExportedColumnQualifiedName(), startFrom, pageSize);

                    if( exportedEntities != null )
                    {
                        for (DatabaseColumnElement col : exportedEntities)
                        {
                            exportedGuids.add(col.getReferencedColumnGUID());
                        }
                    }


                    for( String str : importedGuids )
                    {
                        DatabaseForeignKeyProperties linkProps = new DatabaseForeignKeyProperties();
                        for( String s : exportedGuids )
                            this.getContext().addForeignKeyRelationship(str, s, linkProps);
                    }

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

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_FOREIGN_KEYS.getMessageDefinition(error.getClass().getName(),
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
                String tableGUID = this.getContext().createDatabaseView(schemaGUID, viewProps);
                addColumns( view, tableGUID );
            }


        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
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

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_VIEWS.getMessageDefinition(error.getClass().getName(),
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

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
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
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {

            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
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
    private void addColumns(PostgresTable table, String tableGUID) throws ConnectorCheckedException
    {
        String methodName = "addColumns";
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
                String colGUID = this.getContext().createDatabaseColumn(tableGUID, colProps);

                if (primaryKeys.contains(col.getColumn_name()))
                {
                    DatabasePrimaryKeyProperties keyProps = new DatabasePrimaryKeyProperties();
                    keyProps.setName(col.getColumn_name());
                    getContext().setPrimaryKeyOnColumn(colGUID, keyProps);
                }
            }
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.ERROR_READING_COLUMNS.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_COLUMNS.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PROPERTY.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param col     the postgrews attributes of the column
     * @param guid the GUID of the owning table
     * @param primaryKeys a list of the attributes for each primary key
     * @throws ConnectorCheckedException
     */
    private void addColumn( PostgresColumn col, String guid, List<String> primaryKeys ) throws ConnectorCheckedException
    {
        String methodName= "addColumn";
        try
        {
            DatabaseColumnProperties colProps = new DatabaseColumnProperties();
            colProps.setDisplayName(col.getColumn_name());
            colProps.setQualifiedName(col.getQualifiedName());
            colProps.setAdditionalProperties(col.getProperties());

            String colGUID = this.getContext().createDatabaseColumn(guid, colProps);

            if (primaryKeys.contains(col.getColumn_name()))
            {
                DatabasePrimaryKeyProperties keyProps = new DatabasePrimaryKeyProperties();
                keyProps.setName(col.getColumn_name());
                this.getContext().setPrimaryKeyOnColumn(colGUID, keyProps);
            }
        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (PropertyServerException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                        error);
            }

            throw error;
        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName,error);

        }

    }
}


