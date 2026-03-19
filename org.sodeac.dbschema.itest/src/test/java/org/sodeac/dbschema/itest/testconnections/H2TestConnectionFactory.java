package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.sodeac.dbschema.itest.Statics;
import org.sodeac.dbschema.itest.TestConnection;

import lombok.val;

public class H2TestConnectionFactory extends AbstractTestConnectionFactory
{
    @Serial
    private static final long serialVersionUID = 1L;

    public H2TestConnectionFactory(final Map<String, Boolean> createdSchema, @NonNull final String schemaName)
    { // toUpperCase() important for H2!
        super(createdSchema, schemaName.toUpperCase(Locale.ROOT));
    }

    public static TestConnection create(final Map<String, Boolean> createdSchema, final String schemaName)
            throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(Statics.ENABLED_H2);
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

    @Override
    public TestConnection call() throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(Statics.ENABLED_H2);
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

        if(this.createdSchema.get("H2_" + this.schemaName) == null)
        {
            this.createdSchema.put("H2_" + this.schemaName, true);

            final PreparedStatement prepStat =
                    testConnection.connection.prepareStatement("CREATE SCHEMA " + this.schemaName);
            prepStat.executeUpdate();
            prepStat.close();
        }

        testConnection.connection.setSchema(this.schemaName);
        testConnection.dbmsSchemaName = this.schemaName;
        return testConnection;
    }
}