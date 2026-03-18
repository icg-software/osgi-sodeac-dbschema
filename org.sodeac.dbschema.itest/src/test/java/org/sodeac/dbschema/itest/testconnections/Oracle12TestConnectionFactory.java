package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.Statics;
import org.sodeac.dbschema.itest.TestConnection;

public class Oracle12TestConnectionFactory extends AbstractTestConnectionFactory
{
    @Serial
    private static final long serialVersionUID = 1L;

    public Oracle12TestConnectionFactory(final Map<String, Boolean> createdSchema, final String schemaName)
    {
        super(createdSchema, schemaName);
    }

    @Override
    public TestConnection call() throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(Statics.ENABLED_ORACLE_12);
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

            if(this.createdSchema.get("ORACLE_" + this.schemaName) == null)
            {
                this.createdSchema.put("ORACLE_" + this.schemaName, true);

                final Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.178.45:1521/xe", "system", "oracle");

                PreparedStatement prepStat = connection.prepareStatement("CREATE USER " + this.schemaName.toUpperCase() + " IDENTIFIED BY sodeac DEFAULT TABLESPACE USERS PROFILE DEFAULT");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT CONNECT TO " + this.schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT RESOURCE TO " + this.schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                prepStat = connection.prepareStatement("GRANT DBA TO " + this.schemaName.toUpperCase() + "  WITH ADMIN OPTION");
                prepStat.executeUpdate();
                prepStat.close();

                connection.close();
            }
            testConnection.connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.178.45:1521/xe", this.schemaName.toUpperCase(), "sodeac");
            testConnection.connection.setSchema(this.schemaName.toUpperCase());
            testConnection.dbmsSchemaName = this.schemaName.toUpperCase();

            return testConnection;
        }
        catch (final SQLException e)
        {
            e.printStackTrace();
            throw e;
        }
    }
}