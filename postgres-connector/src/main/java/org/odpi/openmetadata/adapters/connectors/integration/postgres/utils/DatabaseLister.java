package org.odpi.openmetadata.adapters.connectors.integration.postgres.utils;

import org.odpi.openmetadata.accessservices.datamanager.client.DatabaseManagerClient;
import org.odpi.openmetadata.accessservices.datamanager.metadataelements.DatabaseElement;
import org.odpi.openmetadata.frameworks.connectors.ffdc.InvalidParameterException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.PropertyServerException;
import org.odpi.openmetadata.frameworks.connectors.ffdc.UserNotAuthorizedException;

import java.util.List;


class DatabaseLister
{

     public static void main( String[] args )
     {
        DatabaseManagerClient client;
        try
        {
            client = new DatabaseManagerClient( "postgresTargetServer", "https://localhost:9443");
            List<DatabaseElement> databases = client.findDatabases("garygeek", "postgres", 1, 100 );

            for( DatabaseElement element : databases)
            {
                System.out.println( element.toString() );
            }
        }
        catch (InvalidParameterException | UserNotAuthorizedException | PropertyServerException e)
        {
            e.printStackTrace();
        }
        
     }

}
