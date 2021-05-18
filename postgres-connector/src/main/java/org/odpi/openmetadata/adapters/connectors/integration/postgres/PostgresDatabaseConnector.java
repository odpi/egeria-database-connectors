package org.odpi.openmetadata.adapters.connectors.integration.postgres;

/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseColumnElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseSchemaElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseTableElement;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseViewElement;
import org.odpi.openmetadata.accessservices.datamanager.properties.*;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.AlreadyHandledException;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.ExceptionHandler;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.PostgresConnectorAuditCode;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.ffdc.PostgresConnectorErrorCode;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.mapper.PostgresMapper;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresColumn;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresDatabase;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresForeignKeyLinks;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresSchema;
import org.odpi.openmetadata.adapters.connectors.integration.postgres.properties.PostgresTable;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;
import org.odpi.openmetadata.integrationservices.database.connector.DatabaseIntegratorConnector;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PostgresDatabaseConnector extends DatabaseIntegratorConnector
{
    final int startFrom = 0;
    final int pageSize = 0;

    @Override
    public void refresh() throws ConnectorCheckedException
    {
        String methodName = "PostgresConnector.refresh";

        PostgresSourceDatabase source = new PostgresSourceDatabase(connectionProperties);
        try
        {
            /*
            get a list of databases currently hosted in postgres
            and a list of databases already known by egeria
             */
            List<PostgresDatabase> postgresDatabases = source.getDabases();
            List<DatabaseElement> egeriaDatabases = getContext().getMyDatabases(startFrom, pageSize);

            /*
            first we remove any egeria databases that are no longer present in postgres
             */
            deleteDatabases( postgresDatabases, egeriaDatabases );

            for (PostgresDatabase postgresDatabase : postgresDatabases)
            {
                boolean found = false;
                if (egeriaDatabases == null  )
                {
                    if( postgresDatabases.size() > 0 )
                    {
                        /*
                    we have no databases in egeria
                    so all databases are new
                     */
                        addDatabase(postgresDatabase);
                    }
                }
                else
                {
                    /*
                    check if the database is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseElement egeriaDatabase : egeriaDatabases)
                    {

                        String egeriaQN =  egeriaDatabase.getDatabaseProperties().getQualifiedName();
                        String postgresQN = postgresDatabase.getQualifiedName();

                        if (egeriaQN.equals(postgresQN))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateDatabase(postgresDatabase, egeriaDatabase);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                        addDatabase(postgresDatabase);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(  methodName,
                                        PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName,
                                                                                                                error.getClass().getName(),
                                                                                                                error.getMessage()),
                                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (InvalidParameterException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (UserNotAuthorizedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (ConnectorCheckedException error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw error;
        }
        catch (AlreadyHandledException error)
        {
            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ALREADY_HANDLED_EXCEPTION.getMessageDefinition(error.getClass().getName(),
                    error.getMessage()),
                    this.getClass().getName(),
                    methodName, error);

        }
        catch (Exception error)
        {
            if (this.auditLog != null)
            {
                auditLog.logException(methodName,
                        PostgresConnectorAuditCode.UNEXPECTTED_ERROR.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                        error);
            }

            throw new ConnectorCheckedException(PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName),
                    this.getClass().getName(),
                    methodName, error);

        }

    }


    /**
     * Trawls through a database updating a database where necessary
     *
     * @param postgresDatabase the bean properties of a Postgres Database
     * @param egeriaDatabase   the egeria database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateDatabase(PostgresDatabase postgresDatabase, DatabaseElement egeriaDatabase) throws AlreadyHandledException
    {
        String methodName = "updateDatabase";

        try
        {
            if (egeriaDatabase != null)
            {
                String guid = egeriaDatabase.getElementHeader().getGUID();
                /*
                have the properties of the database entity changed
                 */
                if (!postgresDatabase.isEquivalent(egeriaDatabase))
                {
                    /*
                    then we need to update the entity properties
                     */
                    DatabaseProperties props = PostgresMapper.getDatabaseProperties(postgresDatabase);
                    getContext().updateDatabase(guid, props);

                }

                /*
                now trawl through the rest of the schema
                updating where necessary
                 */
                updateSchemas(guid, postgresDatabase.getName());
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }

    /**
     * iterates over the database schemas updating where necessary
     *
     * @param databaseGUID   the egeria database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateSchemas(String databaseGUID, String name) throws AlreadyHandledException
    {
        String methodName = "updateSchemas";
        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

        try
        {
               /*
            get a list of databases schema currently hosted in postgres
            and remove any databases schemas that have been dropped since the last refresh
             */
            List<PostgresSchema> postgresSchemas = source.getDatabaseSchema(name);
            List<DatabaseSchemaElement> egeriaSchemas = getContext().getSchemasForDatabase(databaseGUID, startFrom, pageSize);

            if( egeriaSchemas != null )
            {
                deleteSchemas( postgresSchemas, egeriaSchemas);
            }

            for (PostgresSchema postgresSchema : postgresSchemas)
            {
                boolean found = false;
                /*
                we have no schemas in egeria
                so all schemas are new
                 */
                if (egeriaSchemas == null)
                {
                    if( postgresSchemas.size() > 0 )
                    {
                        addSchemas(name, databaseGUID);
                    }
                }
                else
                {
                    /*
                    check if the schema is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseSchemaElement egeriaSchema : egeriaSchemas)
                    {
                        if (egeriaSchema.getDatabaseSchemaProperties().getQualifiedName().equals(postgresSchema.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateSchema(postgresSchema, egeriaSchema);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                        addSchema(postgresSchema, databaseGUID);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));

        }

        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }

        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
    }

    /**
     * Changes the properties of an egeria schema entity
     *
     * @param postgresSchema            the Postgres Schema properties
     * @param egeriaSchema          the egeria schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateSchema( PostgresSchema postgresSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {
        String methodName = "updateSchema";
        try
        {
            if ( !postgresSchema.isEquivalent(egeriaSchema) )
            {
                DatabaseSchemaProperties props = PostgresMapper.getSchemaProperties(postgresSchema);
                getContext().updateDatabaseSchema(egeriaSchema.getElementHeader().getGUID(), props);
            }
            updateTables(postgresSchema, egeriaSchema);
            updateViews(postgresSchema, egeriaSchema);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * @param postgresSchema the postgres schema bean
     * @param egeriaSchema   the egeria schema bean
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTables(PostgresSchema postgresSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {

        final String methodName = "updateTables";

        String schemaGuid = egeriaSchema.getElementHeader().getGUID();
        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

        try
        {
            /*
            get a list of databases tables currently hosted in postgres
            and remove any tables that have been dropped since the last refresh
             */
            List<PostgresTable> postgresTables = source.getTables(postgresSchema.getSchema_name());
            List<DatabaseTableElement> egeriaTables = getContext().getTablesForDatabaseSchema(schemaGuid, startFrom, pageSize);

            /*
            remove tables from egeria that are no longer needed
             */
            deleteTables( postgresTables, egeriaTables);

            for (PostgresTable postgresTable : postgresTables)
            {
                boolean found = false;
                /*
                we have no tables in egeria but we do have tables in postgres
                so all tables are new
                 */
                if (egeriaTables == null)
                {
                    if( postgresTables.size() > 0 )
                    {
                        addTable(postgresTable, schemaGuid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseTableElement egeriaTable : egeriaTables)
                    {
                        if (egeriaTable.getDatabaseTableProperties().getQualifiedName().equals(postgresTable.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateTable(postgresTable, egeriaTable);
                            break;
                        }
                    }
                    /*
                    this is a new database so add it
                     */
                    if (!found)
                    {
                       addTable(postgresTable, schemaGuid);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * @param postgresTable  the postgres table attributes to be added
     * @param egeriaTable    the GUID of the schema to which the table will be linked
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTable(PostgresTable postgresTable, DatabaseTableElement egeriaTable) throws AlreadyHandledException
    {
        String methodName = "updateTable";

        try
        {
            if( postgresTable.isEquivalent( egeriaTable) )
            {
                DatabaseTableProperties props = PostgresMapper.getTableProperties(postgresTable);
                getContext().updateDatabaseTable(egeriaTable.getElementHeader().getGUID(), props);
            }

            updateTableColumns(postgresTable, egeriaTable);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }

    }


    /**
     * @param postgresSchema the postgres schema bean
     * @param egeriaSchema   the egeria schema bean
     * @throws AlreadyHandledException this exception has already been logged
     */

    private void updateViews(PostgresSchema postgresSchema, DatabaseSchemaElement egeriaSchema) throws AlreadyHandledException
    {
        final String methodName = "updateViews";

        String schemaGuid = egeriaSchema.getElementHeader().getGUID();
        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

        try
        {
            /*
            get a list of databases views currently hosted in postgres
            and remove any tables that have been dropped since the last refresh
             */
            List<PostgresTable> postgresViews = source.getViews(postgresSchema.getSchema_name());
            List<DatabaseViewElement> egeriaViews = getContext().getViewsForDatabaseSchema(schemaGuid, startFrom, pageSize);

            deleteViews( postgresViews, egeriaViews);
            for (PostgresTable postgresView : postgresViews)
            {
                boolean found = false;
                /*
                we have no views in egeria
                so all views are new
                 */
                if (egeriaViews == null)
                {
                    if( postgresViews.size() > 0)
                    {
                        addView(postgresView, schemaGuid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseViewElement egeriaView : egeriaViews)
                    {
                        if (egeriaView.getDatabaseViewProperties().getQualifiedName().equals(postgresView.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateView(postgresView, egeriaView);
                            break;
                        }
                    }
                    /*
                    this is a new database view so add it
                     */
                    if (!found)
                    {
                        addView(postgresView, schemaGuid);
                    }
                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }

        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * @param postgresTable         the postgres table attributes to be added
     * @param egeriaView    te GUID of the schema to which the table will be linked
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateView(PostgresTable postgresTable, DatabaseViewElement egeriaView) throws AlreadyHandledException
    {
        String methodName = "updateView";

        try
        {
            if( !postgresTable.isEquivalent( egeriaView) )
            {
                DatabaseViewProperties props = PostgresMapper.getViewProperties(postgresTable);
                getContext().updateDatabaseView(egeriaView.getElementHeader().getGUID(), props);
            }

            updateViewColumns(postgresTable, egeriaView);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(error.getClass().getName()));
        }

    }


    /**
     * @param postgresTable         the postgres table which contains the columns to be updates
     * @param  egeriaTable  the column data from egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateTableColumns(PostgresTable postgresTable, DatabaseTableElement egeriaTable) throws AlreadyHandledException
    {
        final String methodName = "updateTableColumns";
        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);
        String tableGuid = egeriaTable.getElementHeader().getGUID();
        try
        {
            List<PostgresColumn> postgresColumns = source.getColumns(postgresTable.getTable_name());
            List<DatabaseColumnElement> egeriaColumns = getContext().getColumnsForDatabaseTable(tableGuid, startFrom, pageSize);
            List<String> primarykeys = source.getPrimaryKeyColumnNamesForTable( postgresTable.getTable_name());

                if( egeriaColumns != null && postgresColumns.size() > 0)
                {
                    deleteTableColumns(postgresColumns, egeriaColumns);
                }

                for (PostgresColumn postgresColumn : postgresColumns)
                {
                    boolean found = false;
                    /*
                    we have no columns in egeria
                    so all columns are new
                     */
                    if (egeriaColumns == null)
                    {
                        if( postgresColumns.size() > 0 )
                        {
                            addColumn(postgresColumn, tableGuid);
                        }
                    }
                    else
                    {
                        /*
                        check if the database table is known to egeria
                        and needs to be updated
                         */
                        for (DatabaseColumnElement egeriaColumn : egeriaColumns)
                        {
                            if (egeriaColumn.getDatabaseColumnProperties().getQualifiedName().equals(postgresColumn.getQualifiedName()))
                            {
                            /*
                            we have found an exact instance to update
                             */
                                found = true;
                                //TODO
                             //   updateColumn(postgresColumn, egeriaColumn);
                                break;
                            }

                            if( primarykeys.contains(egeriaColumn.getDatabaseColumnProperties().getDisplayName() ))
                            {
                                DatabasePrimaryKeyProperties props = new DatabasePrimaryKeyProperties();
                                getContext().setPrimaryKeyOnColumn(egeriaColumn.getElementHeader().getGUID(), props);
                            }
                            else
                            {
                                //was this a primary key previously.
                                if( egeriaColumn.getPrimaryKeyProperties() != null )
                                {
                                    getContext().removePrimaryKeyFromColumn( egeriaColumn.getElementHeader().getGUID());
                                }

                            }

                        }
                        /*
                        this is a new database so add it
                         */
                        if (!found)
                        {
                           addColumn(postgresColumn, tableGuid);
                        }
                    }
                }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }

    /**
     * @param postgresTable         the postgres table which contains the columns to be updates
     * @param  egeriaTable  the column data from egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateViewColumns(PostgresTable postgresTable, DatabaseViewElement egeriaTable) throws AlreadyHandledException
    {
        final String methodName = "updateViewColumns";

        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);
        String guid = egeriaTable.getElementHeader().getGUID();
        try
        {
            List<PostgresColumn> postgresColumns = source.getColumns(postgresTable.getTable_name());
            List<DatabaseColumnElement> egeriaColumns = getContext().getColumnsForDatabaseTable(egeriaTable.getElementHeader().getGUID(), startFrom, pageSize);

            if( egeriaColumns != null )
            {
                deleteViewColumns(postgresColumns, egeriaColumns);
            }

            for (PostgresColumn postgresColumn : postgresColumns)
            {
                boolean found = false;
                /*
                we have no tables in egeria
                so all tables are new
                 */
                if (egeriaColumns == null)
                {
                    if(postgresColumns.size() > 0 )
                    {
                        addColumn(postgresColumn, guid);
                    }
                }
                else
                {
                    /*
                    check if the database table is known to egeria
                    and needs to be updated
                     */
                    for (DatabaseColumnElement egeriaColumn : egeriaColumns)
                    {
                        if (egeriaColumn.getDatabaseColumnProperties().getQualifiedName().equals(postgresColumn.getQualifiedName()))
                        {
                        /*
                        we have found an exact instance to update
                         */
                            found = true;
                            updateColumn(postgresColumn, egeriaColumn);
                            break;
                        }

                    }
                    /*
                    this is a new column so add it
                     */
                    if (!found)
                    {
                        addColumn(postgresColumn, guid);
                    }

                }


            }

        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }


    /**
     * @param postgresCol           the postgres column
     * @param  egeriaCol            the column data from egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void updateColumn(PostgresColumn postgresCol, DatabaseColumnElement egeriaCol ) throws AlreadyHandledException
    {
        String methodName = "updateColumn";

        try
        {
            if( !postgresCol.isEquivalent( egeriaCol))
            {
                DatabaseColumnProperties props = PostgresMapper.getColumnProperties( postgresCol );
                getContext().updateDatabaseColumn(egeriaCol.getElementHeader().getGUID(), props);
            }

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param db the postgres attributes of the database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addDatabase(PostgresDatabase db) throws AlreadyHandledException
    {
        String methodName = "addDatabase";
      try
        {
         /*
         new database so build the database in egeria
         */
            DatabaseProperties dbProps = PostgresMapper.getDatabaseProperties(db);
            String guid = this.getContext().createDatabase(dbProps);
            addSchemas(db.getName(), guid);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * Adds schema entities to egeria for a given database
     *
     * @param dbName the name of the database
     * @param dbGUID the GUID of the database entity to attach the schemas
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addSchemas(String dbName, String dbGUID) throws AlreadyHandledException
    {

        String methodName = "addSchemas";

        try
        {
            PostgresSourceDatabase sourceDB = new PostgresSourceDatabase(this.connectionProperties);
            List<PostgresSchema> schemas = sourceDB.getDatabaseSchema(dbName);
            for (PostgresSchema sch : schemas)
            {
                addSchema(sch, dbGUID);
            }

        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));
        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param sch     the postgres schema attributes to be
     * @param dbGuidd the egeria GUID of the database
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addSchema(PostgresSchema sch, String dbGuidd) throws AlreadyHandledException
    {
        String methodName = "addSchema";

        try
        {
            DatabaseSchemaProperties schemaProps = PostgresMapper.getSchemaProperties(sch);

            String schemaGUID = getContext().createDatabaseSchema(dbGuidd, schemaProps);
            addTables(sch.getSchema_name(), schemaGUID);
            addViews( sch.getSchema_name(), schemaGUID);
            addForeignKeys(sch);
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param schemaName the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addTables(String schemaName, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addTables";

        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

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
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }

    }

    /**
     * creates an egeria DatabaseTable entity for a given Postgres Table
     *
     * @param table      the postgres schema attributes to be
     * @param schemaGUID the egeria GUID of the schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addTable(PostgresTable table, String schemaGUID) throws AlreadyHandledException
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
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }

    /**
     * creates an egeria DatabaseView entity for a given Postgres Table
     * in postgres views are tables
     *
     * @param view       the postgres view properties
     * @param schemaGUID the egeria GUID of the schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addView(PostgresTable view, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addView";

        try
        {
            DatabaseViewProperties props = PostgresMapper.getViewProperties(view);
            String tableGUID = this.getContext().createDatabaseView(schemaGUID, props);
            addColumns(view.getTable_name(), tableGUID);
        } catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        } catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }

    }


    /**
     * add the foreign keys to egeria
     * for a schema from postgres and adds the data to egeria
     *
     * @param schema the attributes of the schema which owns the tables
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addForeignKeys(PostgresSchema schema) throws AlreadyHandledException
    {
        String methodName = "addForeignKeys";

        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

        try
        {

            List<PostgresTable> tables = source.getTables(schema.getSchema_name());
            for (PostgresTable table : tables)
            {
                List<PostgresForeignKeyLinks> foreignKeys = source.getForeginKeyLinksForTable(table.getTable_name());
                List<String> importedGuids = new ArrayList<>();
                List<String> exportedGuids = new ArrayList<>();

                for (PostgresForeignKeyLinks link : foreignKeys)
                {
                    List<DatabaseColumnElement> importedEntities = getContext().findDatabaseColumns(link.getImportedColumnQualifiedName(), startFrom, pageSize);

                    if (importedEntities != null)
                    {
                        for (DatabaseColumnElement col : importedEntities)
                        {
                            importedGuids.add(col.getReferencedColumnGUID());
                        }
                    }

                    List<DatabaseColumnElement> exportedEntities = this.getContext().findDatabaseColumns(link.getExportedColumnQualifiedName(), startFrom, pageSize);

                    if (exportedEntities != null)
                    {
                        for (DatabaseColumnElement col : exportedEntities)
                        {
                            exportedGuids.add(col.getReferencedColumnGUID());
                        }
                    }


                    for (String str : importedGuids)
                    {
                        DatabaseForeignKeyProperties linkProps = new DatabaseForeignKeyProperties();
                        for (String s : exportedGuids)
                            getContext().addForeignKeyRelationship(str, s, linkProps);
                    }

                }
            }
        }
        catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param schemaName the attributes of the schema which owns the tables
     * @param schemaGUID the GUID of the owning schema
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addViews(String schemaName, String schemaGUID) throws AlreadyHandledException
    {
        String methodName = "addViews";

        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);

        try
        {
            List<PostgresTable> views = source.getViews(schemaName);

            for (PostgresTable view : views)
            {
                addView(view, schemaGUID);
            }


        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }
    }

    /**
     * mapping function that reads tables, columns and primary keys
     * for a schema from postgres and adds the data to egeria
     *
     * @param tableName the name of the parent table
     * @param tableGUID the GUID of the owning table
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void addColumns(String tableName, String tableGUID) throws AlreadyHandledException
    {
        String methodName = "addColumns";

        PostgresSourceDatabase source = new PostgresSourceDatabase(this.connectionProperties);
        try
        {
            List<PostgresColumn> cols = source.getColumns(tableName);

            for (PostgresColumn col : cols)
            {
                addColumn(col, tableGUID);
            }
        } catch (SQLException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.ERROR_READING_POSTGRES.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.ERROR_READING_FROM_POSTGRES.getMessageDefinition(methodName));

        }

    }

    /**
     * mapping function that reads columns and primary keys
     * for a schema from postgres and creates
     *
     * @param col         the postgres attributes of the column
     * @param guid        the GUID of the owning table
     * @throws AlreadyHandledException allows the exception to be passed up the stack, without additional handling
     */
    private void addColumn(PostgresColumn col, String guid) throws AlreadyHandledException
    {
        String methodName = "addColumn";

        try
        {
            DatabaseColumnProperties colProps = PostgresMapper.getColumnProperties(col);
            this.getContext().createDatabaseColumn(guid, colProps);

        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }





    /**
     * Checks if any databases need to be removed from egeria
     *
     * @param postgresDatabases            a list of the bean properties of a Postgres Database
     * @param egeriaDatabases    a list of the Databases already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteDatabases(List<PostgresDatabase> postgresDatabases, List<DatabaseElement> egeriaDatabases) throws AlreadyHandledException
    {
        String methodName = "deleteDatabases";

        try
        {
            if (egeriaDatabases != null)
            {
                /*
                for each database already known to egeria
                 */
                for (Iterator<DatabaseElement> itr = egeriaDatabases.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseElement egeriaDatabase = itr.next();
                    String knownName = egeriaDatabase.getDatabaseProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresDatabase postgresDatabase : postgresDatabases)
                    {
                        String sourceName = postgresDatabase.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next database
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the database from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabase(egeriaDatabase.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        } catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * Checks if any schemas need to be removed from egeria
     *
     * @param postgresSchemas            a list of the bean properties of a Postgres schemas
     * @param egeriaSchemas    a list of the Databases already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteSchemas(List<PostgresSchema> postgresSchemas, List<DatabaseSchemaElement> egeriaSchemas) throws AlreadyHandledException
    {
        String methodName = "deleteSchemas";

        try
        {
            if (egeriaSchemas != null)
            {
                /*
                for each schema already known to egeria
                 */
                for (Iterator<DatabaseSchemaElement> itr = egeriaSchemas.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseSchemaElement egeriaSchema = itr.next();

                    String knownName = egeriaSchema.getDatabaseSchemaProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresSchema postgresSchema : postgresSchemas)
                    {
                        String sourceName = postgresSchema.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next schema
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the schema from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseSchema(egeriaSchema.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        } catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * Checks if any schemas need to be removed from egeria
     *
     * @param postgresTables            a list of the bean properties of a Postgres schemas
     * @param egeriaTables    a list of the Databases already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteTables(List<PostgresTable> postgresTables, List<DatabaseTableElement> egeriaTables) throws AlreadyHandledException
    {
        String methodName = "deleteTables";
        try
        {
            if (egeriaTables != null)
            {
                /*
                for each table already known to egeria
                 */
                for (Iterator<DatabaseTableElement> itr = egeriaTables.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseTableElement egeriaTable = itr.next();
                    String knownName = egeriaTable.getDatabaseTableProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresTable postgresTable : postgresTables)
                    {
                        String sourceName = postgresTable.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next table
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the table from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseTable(egeriaTable.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }

    /**
     * Checks if any views need to be removed from egeria
     *
     * @param postgresViews            a list of the bean properties of a Postgres views
     * @param egeriaViews               a list of the  views already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteViews(List<PostgresTable> postgresViews, List<DatabaseViewElement> egeriaViews) throws AlreadyHandledException
    {
        String methodName = "deleteViews";

        try
        {
            if (egeriaViews != null)
            {
                /*
                for each view already known to egeria
                 */
                for (Iterator<DatabaseViewElement> itr = egeriaViews.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseViewElement egeriaView = itr.next();

                    String knownName = egeriaView.getDatabaseViewProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresTable postgresView : postgresViews)
                    {
                        String sourceName = postgresView.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next table
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the table from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaView.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }


    /**
     * Checks if any columns need to be removed from egeria
     *
     * @param postgresColumns            a list of the bean properties of a Postgres cols
     * @param egeriaColumns               a list of the  cols already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteTableColumns(List<PostgresColumn> postgresColumns, List<DatabaseColumnElement> egeriaColumns) throws AlreadyHandledException
    {
        String methodName = "deleteTableColumns";

        try
        {
            if (egeriaColumns != null)
            {
                /*
                for each column already known to egeria
                 */
                for (Iterator<DatabaseColumnElement> itr = egeriaColumns.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseColumnElement egeriaColumn = itr.next();

                    String knownName = egeriaColumn.getDatabaseColumnProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresColumn postgresColumn : postgresColumns)
                    {
                        String sourceName = postgresColumn.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next column
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the table from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaColumn.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        } catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        } catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        } catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }


    /**
     * Checks if any columns need to be removed from egeria
     *
     * @param postgresColumns            a list of the bean properties of a Postgres cols
     * @param egeriaColumns               a list of the  cols already known to egeria
     * @throws AlreadyHandledException this exception has already been logged
     */
    private void deleteViewColumns(List<PostgresColumn> postgresColumns, List<DatabaseColumnElement> egeriaColumns) throws AlreadyHandledException
    {
        String methodName = "deleteViewColumns";

        try
        {
            if (egeriaColumns != null)
            {
                /*
                for each column already known to egeria
                 */
                for (Iterator<DatabaseColumnElement> itr = egeriaColumns.iterator(); itr.hasNext();)
                {
                    boolean found = false;
                    DatabaseColumnElement egeriaColumn = itr.next();

                    String knownName = egeriaColumn.getDatabaseColumnProperties().getQualifiedName();
                    /*
                    check that the database is still present in postgres
                     */
                    for (PostgresColumn postgresColumn : postgresColumns)
                    {
                        String sourceName = postgresColumn.getQualifiedName();
                        if (sourceName.equals(knownName))
                        {
                            /*
                            if found then check the next column
                             */
                            found = true;
                            break;
                        }
                    }
                        /*
                        not found in postgres , so delete the table from egeria
                         */
                    if( !found)
                    {
                        getContext().removeDatabaseView(egeriaColumn.getElementHeader().getGUID(), knownName);
                        itr.remove();
                    }

                }
            }
        }
        catch (InvalidParameterException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.INVALID_PARAMETER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (PropertyServerException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.PROPERTY_SERVER_EXCEPTION.getMessageDefinition(methodName));
        }
        catch (UserNotAuthorizedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.USER_NOT_AUTORIZED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.USER_NOT_AUTHORIZED_EXCEPTION.getMessageDefinition(methodName));

        }
        catch (ConnectorCheckedException error)
        {
            ExceptionHandler.handleException(auditLog,
                    this.getClass().getName(),
                    methodName, error,
                    PostgresConnectorAuditCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName, error.getClass().getName(), error.getMessage()),
                    PostgresConnectorErrorCode.CONNECTOR_CHECKED_EXCEPTION.getMessageDefinition(methodName));
        }
    }
}
