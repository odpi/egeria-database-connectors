/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */

package org.odpi.openmetadata.adapters.connectors.datastore.postgres;

import org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties.PostgresDatabase;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
The DatabaseServer class abstracts away the connection to the database host system which is needed to gain a list of databases
 */
public class DatabaseServer
{

    /*
    returns a list of databases served ny a particular server
    connection url MUST be in format dbc:postgresql://host:port

     * @param props contains the database connection properties
     * @return A list of datbase attributes hosted by the host serever
     * @throws SQLException thrown by the JDBC Driver

     */

    public static List<PostgresDatabase> getDabaseNames(ConnectionProperties props) throws SQLException
    {
        ArrayList<PostgresDatabase> databaseNames = new ArrayList();

        /*
        This assumes that the connection url contains user/pwd etc
         */
        try( Connection connection  = DriverManager.getConnection(props.getURL( ));
             PreparedStatement ps = connection.prepareStatement("SELECT VERSION(), * FROM pg_database WHERE datistemplate = false;");
             ResultSet rs = ps.executeQuery();
        )
        {

            while (rs.next()) {
                databaseNames.add( new PostgresDatabase(rs.getString("Name"),
                                                                rs.getString("Owner"),
                                                                rs.getString("Encoding"),
                                                                rs.getString("Collate"),
                                                                rs.getString("Ctype"),
                                                                rs.getString("Access privileges "),
                                                                rs.getString ( "version" ) ) );
            }
        }

        return databaseNames;
    }
}
