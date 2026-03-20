package org.sodeac.dbschema.itest.testconnections;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.TestConnection;

import lombok.val;

public final class H2TestConnectionFactory
{
    private H2TestConnectionFactory() { }

    public static TestConnection create(final Map<String, Boolean> createdSchema, final String schemaName, final boolean isEnabled)
            throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(isEnabled);
        if(!testConnection.enabled)
        {
            return testConnection;
        }

        try
        {
            Class.forName("org.h2.Driver").newInstance();
        }
        catch (final Exception e) { }

        testConnection.connection = DriverManager.getConnection("jdbc:h2:./../../../test", "sa", "sa");

        // toUpperCase() important for H2!
        val schemaName2 = schemaName.toUpperCase();
        if(createdSchema.get("H2_" + schemaName2) == null)
        {
            createdSchema.put("H2_" + schemaName2, true);

            final PreparedStatement prepStat =
                    testConnection.connection.prepareStatement("CREATE SCHEMA " + schemaName2);
            prepStat.executeUpdate();
            prepStat.close();
        }

        testConnection.connection.setSchema(schemaName2);
        testConnection.dbmsSchemaName = schemaName2;
        return testConnection;
    }
}