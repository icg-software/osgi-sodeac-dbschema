package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.TestConnection;

public class PostgresTestConnectionFactory extends AbstractTestConnectionFactory
{
    @Serial
    private static final long serialVersionUID = 1L;

    public PostgresTestConnectionFactory(final Map<String, Boolean> createdSchema, final String schemaName)
    {
        super(createdSchema, schemaName);
    }

    @Override
    public TestConnection call() throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(true);
        if(!testConnection.enabled)
        {
            return testConnection;
        }

        // docker run --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sodeac -d -p 5432:5432 postgres

        // docker exec -it postgres bash:
        // mkdir /var/lib/postgresql/data/sodeacdata
        // mkdir /var/lib/postgresql/data/sodeacindex
        // chown postgres.postgres /var/lib/postgresql/data/sodeacdata
        // chown postgres.postgres /var/lib/postgresql/data/sodeacindex

        // psql -h 127.0.0.1 -U postgres --dbname=postgres ::

        // CREATE USER sodeac with SUPERUSER CREATEDB CREATEROLE INHERIT REPLICATION LOGIN PASSWORD 'sodeac';
        // CREATE TABLESPACE sodeacdata OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacdata';
        // CREATE TABLESPACE sodeacindex OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacindex';
        //

        // CREATE SCHEMA IF NOT EXISTS sodeac1 AUTHORIZATION sodeac;
        try
        {
            Class.forName("org.postgresql.Driver").newInstance();
        }
        catch (final Exception e) { }

        testConnection.connection =
                // DriverManager.getConnection("jdbc:postgresql://192.168.178.45:5432/sodeac", "sodeac", "sodeac");
                DriverManager.getConnection("jdbc:postgresql://localhost/sodeac", "sodeac", "sodeac");

        if(this.createdSchema.get("POSTGRES_" + this.schemaName) == null)
        {
            this.createdSchema.put("POSTGRES_" + this.schemaName, true);

            final PreparedStatement prepStat = testConnection.connection.prepareStatement(
                    "CREATE SCHEMA IF NOT EXISTS " + this.schemaName.toLowerCase() + " AUTHORIZATION sodeac");
            prepStat.executeUpdate();
            prepStat.close();
        }

        testConnection.connection.setSchema(this.schemaName.toLowerCase());
        testConnection.dbmsSchemaName = this.schemaName.toLowerCase();

        return testConnection;
    }
}