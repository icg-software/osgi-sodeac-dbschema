package org.sodeac.dbschema.itest.testconnections;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.TestConnection;

public final class Oracle12TestConnectionFactory
{

    private Oracle12TestConnectionFactory() { }

    public static TestConnection create(final Map<String, Boolean> createdSchema, final String schemaName, final boolean isEnabled)
            throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(isEnabled);
        if(!testConnection.enabled)
        {
            return testConnection;
        }

        // docker run --name oracle -d -p 28080:8080 -p 1521:1521 sath89/oracle-12c

						/*
						 * http://localhost:8080/apex
							workspace: INTERNAL
							user: ADMIN
							password: 0Racle$

							Apex upgrade up to v 5.*

							hostname: localhost
							port: 1521
							sid: xe
							service name: xe
							username: system
							password: oracle

						 */

        // sqlplus system/oracle@//localhost:1521/xe:

        // CREATE TABLESPACE sodeacdata  DATAFILE 'sodeacdata.dbf' SIZE 40M AUTOEXTEND ON NEXT 10M ONLINE;
        // CREATE TABLESPACE sodeacindex  DATAFILE 'sodeacindex.dbf' SIZE 40M AUTOEXTEND ON NEXT 10M ONLINE;

        // CREATE USER SODEAC IDENTIFIED BY sodeac DEFAULT TABLESPACE USERS PROFILE DEFAULT
        // GRANT CONNECT TO SODEAC  WITH ADMIN OPTION
        // GRANT RESOURCE TO SODEAC  WITH ADMIN OPTION
        // GRANT DBA TO SODEAC  WITH ADMIN OPTION

						/*   select
								'drop user '||username||' cascade;'
							from dba_users  WHERE username LIKE 'S00%'
						*/

        try
        {

            try
            {
                Class.forName("oracle.jdbc.OracleDriver").newInstance();
            }
            catch (final Exception e) { }

            if(createdSchema.get("ORACLE_" + schemaName) == null)
            {
                createdSchema.put("ORACLE_" + schemaName, true);

                final Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.178.45:1521/xe", "system", "oracle");

                PreparedStatement prepStat = connection.prepareStatement("CREATE USER " + schemaName.toUpperCase() + " IDENTIFIED BY sodeac DEFAULT TABLESPACE USERS PROFILE DEFAULT");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT CONNECT TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT RESOURCE TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT DBA TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                connection.close();
            }
            testConnection.connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.178.45:1521/xe", schemaName.toUpperCase(), "sodeac");
            testConnection.connection.setSchema(schemaName.toUpperCase());
            testConnection.dbmsSchemaName = schemaName.toUpperCase();

            return testConnection;
        }
        catch (final SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }
}