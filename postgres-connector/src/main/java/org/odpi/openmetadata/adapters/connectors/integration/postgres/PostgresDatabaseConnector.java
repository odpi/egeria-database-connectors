package org.odpi.openmetadata.adapters.connectors.integration.postgres;

/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.*;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.AlreadyHanledException;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.ExceptionHandler;
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
                /*
                we have no databases in egeria
                so all databases are new
                 */
                if( knownDatabases == null )
                {
                    addDatabase(db);
                    continue;
                }
                else
                {
                    /*
                    check if the database is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseElement base : knownDatabases)
                    {
                        if (base.getDatabaseProperties().getQualifiedName().equals(db.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            //TODO
                            found = true;
                            //.updateDatabase(db, knownDatabases);
                            break;
                        }
                    }
                    /*
                    this is a new table so add it
                     */
                    if (found == false)
                    {
                        addDatabase(db);
                    }
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
        catch (AlreadyHanledException error )
        {
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ALREADY_HANDLED_EXCEPTION.getMessageDefinition(error.getClass().getName(),
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
    private void updateDatabase(PostgresDatabase db, List<DatabaseElement> databases) throws ConnectorCheckedException, AlreadyHanledException
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
    private void updateTables(PostgresSchema sch, DatabaseSchemaElement schema) throws ConnectorCheckedException, AlreadyHanledException
    {
        final String methodName = "updateTablesForDatabaseSchema";
        int startFrom = 1;
        int pageSize = 100;

        String schemaGuid = schema.getElementHeader().getGUID();
        PostgresSourceDatabase sourceDatabase = new PostgresSourceDatabase( this.connectionProperties);

        try
        {
            List<PostgresTable> tables = sourceDatabase.getTables(sch.getSchema_name());
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

    private void updateCols(PostgresTable table) throws AlreadyHanledException, ConnectorCheckedException
    {
        final String methodName = "updateCols";
        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);
        try
        {
            List<PostgresColumn> columns = source.getColumnAttributes(table.getTable_name());
            List<String> primaryKeys = source.getPrimaryKeyColumnNamesForTable(table.getTable_name());

            List<DatabaseColumnElement> knownColumns = getContext().getColumnsForDatabaseTable(table.getQualifiedName(), 1, 1000);
            for (DatabaseColumnElement c : knownColumns)
            {
                String knownName = c.getDatabaseColumnProperties().getQualifiedName();
                for (PostgresColumn col : columns)
                {
                    if (col.getQualifiedName().equals(knownName))
                    {
                        break;
                    }
                    /*
                    no longer hosted by the server, so remove
                     */
                    getContext().removeDatabaseColumn(c.getElementHeader().getGUID(), knownName);
                }
            }

            for (PostgresColumn col : columns)
            {
                List<DatabaseColumnElement> colElements = getContext().getDatabaseColumnsByName(col.getQualifiedName(), 1, 1);
                DatabaseColumnElement element = colElements.get(0);
                if (colElements != null)
                {
                    addColumn(col, element.getElementHeader().getGUID(), primaryKeys);
                }
                else
                {
                    updateColumn(col);
                }


            }
        } catch (SQLException error)
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
                    methodName, error);

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
                    methodName, error);

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
                    methodName, error);

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
                    methodName, error);

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
    private void addDatabase(PostgresDatabase db) throws AlreadyHanledException
    {
        String methodName = "addDatabase";
        String guid = "";

        try
        {
         /*
         new database so build the database in egeria
         */
            DatabaseProperties dbProps = PostgresMapper.getDatabaseProperties(db);
            guid = this.getContext().createDatabase(dbProps);
            addSchemas(db.getName(), guid);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }
    }

    /**
     * Adds schema entites to egeria for a given database
     *
     * @param dbName the name of the database
     * @param dbGUID the GUID of the datbase enitity to attach the schemas
     * @throws ConnectorCheckedException
     */
    private void addSchemas(String dbName, String dbGUID) throws AlreadyHanledException
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
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_SCHEMAS.getMessageDefinition(),
                    PostgresConnectorErrorCode.ERROR_READING_SCHEMAS.getMessageDefinition(error.getClass().getName()));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

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
    private void addSchema(PostgresSchema sch, String dbGuidd) throws AlreadyHanledException
    {
        String methodName = "addSchema";
        try
        {
            DatabaseSchemaProperties schemaProps = PostgresMapper.getSchemaProperties(sch);

            String schemaGUID = getContext().createDatabaseSchema(dbGuidd, schemaProps);
            addTables(sch.getSchema_name(), schemaGUID);
            addViews(sch.getSchema_name(), schemaGUID);
            addForeignKeys(sch);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param schemaName     the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws AlreadyHanledException
     */
    private void addTables( String schemaName, String schemaGUID) throws AlreadyHanledException
    {
        String methodName = "addTables";
        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);

        try
        {
            /* add the schema tables */
            List<PostgresTable> tables = source.getTables(schemaName);
            for (PostgresTable table : tables)
            {
                    addTable(table, schemaGUID);
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_TABLES.getMessageDefinition(),
                    PostgresConnectorErrorCode.ERROR_READING_TABLES.getMessageDefinition(error.getClass().getName()));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }

    }

    /**
     * creates an egeria DatabaseTable entity for a given Postgres Table
     *
     * @param table     the postgres schema attributes to be
     * @param schemaGUID the egeria GUID of the schema
     * @throws ConnectorCheckedException
     */
    private void addTable(PostgresTable table, String schemaGUID) throws AlreadyHanledException
    {
        String methodName = "addTable";

        try
        {
            DatabaseTableProperties props = PostgresMapper.getTableProperties(table);
            String tableGUID = this.getContext().createDatabaseTable(schemaGUID, props);
            addColumns(table.getTable_name(), tableGUID);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }

    }

    /**
     * creates an egeria DatabaseView entity for a given Postgres Table
     * in postgres views are tables
     *
     * @param view     the postgres view properties
     * @param schemaGUID the egeria GUID of the schema
     * @throws AlreadyHanledException
     */
    private void addView(PostgresTable view, String schemaGUID) throws AlreadyHanledException
    {
        String methodName = "addTable";

        try
        {
            DatabaseTableProperties props = PostgresMapper.getTableProperties(view);
            String tableGUID = this.getContext().createDatabaseTable(schemaGUID, props);
            addColumns(view.getTable_name(), tableGUID);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }

    }


    /**
     * add the foregin keys to egeria
     * for a schema from postgres and adds the data to egeria
     *
     * @param schema   the attributes of the schema which owns the tables
     * @throws AlreadyHanledException
     */
    private void addForeignKeys(PostgresSchema schema) throws AlreadyHanledException
    {
        String methodName = "addForeignKeys";
        int startFrom = 1;
        int pageSize = 100;

        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);

        try
        {

            List<PostgresTable> tables = source.getTables(schema.getSchema_name());
            for (PostgresTable table : tables)
            {
                List<PostgresForeginKeyLinks> foreginKeys = source.getForeginKeyLinksForTable(table.getTable_name());
                List<String> importedGuids = new ArrayList<>();
                List<String> exportedGuids = new ArrayList<>();

                for (PostgresForeginKeyLinks link : foreginKeys)
                {
                    List<DatabaseColumnElement> importedEntities = getContext().findDatabaseColumns(link.getImportedColumnQualifiedName(), startFrom, pageSize) ;

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
                            getContext().addForeignKeyRelationship(str, s, linkProps);
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param schemaName     the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws ConnectorCheckedException thrown by the JDBC Driver
     */
    private void addViews(String schemaName, String schemaGUID) throws AlreadyHanledException
    {
        String methodName = "addViews";
        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);

        try
        {
            List<PostgresTable> views = source.getViews(schemaName);

            for (PostgresTable view : views)
            {
                addView(view, schemaGUID);
            }


        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_VIEWS.getMessageDefinition(),
                    PostgresConnectorErrorCode.ERROR_READING_VIEWS.getMessageDefinition(error.getClass().getName()));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }
    }

    /**
     * mapping function that reads tables, columns and primmary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param tableName     the name of the parent table
     * @param tableGUID the GUID of the owning table
     * @throws AlreadyHanledException thrown by the JDBC Driver
     */
    private void addColumns(String tableName, String tableGUID) throws AlreadyHanledException
    {
        String methodName = "addColumns";
        PostgresSourceDatabase source = new PostgresSourceDatabase( this.connectionProperties);
        try
        {
            List<String> primaryKeys = source.getPrimaryKeyColumnNamesForTable(tableName);
            List<PostgresColumn> cols = source.getColumnAttributes(tableName);

            for (PostgresColumn col : cols)
            {
                addColumn(col, tableGUID, primaryKeys);
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_COLUMNS.getMessageDefinition(),
                    PostgresConnectorErrorCode.ERROR_READING_COLUMNS.getMessageDefinition(error.getClass().getName()));

        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }

    }

    /**
     * mapping function that reads columns and primmary keys
     * for a schema from postgres and creates
     *
     * @param col     the postgrews attributes of the column
     * @param guid the GUID of the owning table
     * @param primaryKeys a list of the attributes for each primary key
     * @throws AlreadyHanledException allows the exception to be passed up the stack, without additional handling
     */
    private void addColumn( PostgresColumn col, String guid, List<String> primaryKeys ) throws AlreadyHanledException
    {
        String methodName= "addColumn";
        try
        {
            DatabaseColumnProperties colProps = PostgresMapper.getColumnProperties( col );
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
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER.getMessageDefinition(),
                    PostgresConnectorErrorCode.INVALID_PARAMETER.getMessageDefinition(error.getClass().getName()));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED.getMessageDefinition(),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED.getMessageDefinition(error.getClass().getName()));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED.getMessageDefinition(),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED.getMessageDefinition(error.getClass().getName()));
        }
        catch (Exception error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(),
                    PostgresConnectorErrorCode.UNEXPECTED_ERROR.getMessageDefinition(error.getClass().getName()));

        }
    }
}


