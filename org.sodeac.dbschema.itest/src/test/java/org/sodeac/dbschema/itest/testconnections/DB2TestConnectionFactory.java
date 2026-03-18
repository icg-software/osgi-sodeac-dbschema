package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.Statics;
import org.sodeac.dbschema.itest.TestConnection;

public class DB2TestConnectionFactory extends AbstractTestConnectionFactory
{
    @Serial
    private static final long serialVersionUID = 1L;

    public DB2TestConnectionFactory(final Map<String, Boolean> createdSchema, final String schemaName)
    {
        super(createdSchema, schemaName);
    }

    @Override
    public TestConnection call() throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(Statics.ENABLED_DB2);
        if(!testConnection.enabled)
        {
            return testConnection;
        }

        // docker run --name db2 -d -p 50000:50000 -e DB2INST1_PASSWORD=sodeac -e LICENSE=accept  ibmcom/db2express-c:latest db2start
        // docker exec -it db2 bash:
        // su db2inst1
        // db2 create db sodeac

        // CREATE TABLESPACE SODEACDATA MANAGED BY AUTOMATIC STORAGE
        // CREATE TABLESPACE SODEACINDEX MANAGED BY AUTOMATIC STORAGE

        try
        {
            Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance().getClass();
        }
        catch (final Exception e) { }

        testConnection.connection =
                DriverManager.getConnection("jdbc:db2://127.0.0.1:50000/sodeac", "db2inst1", "sodeac");

        if(this.createdSchema.get("DB2_" + this.schemaName) == null)
        {
            this.createdSchema.put("DB2_" + this.schemaName, true);

            final PreparedStatement prepStat = testConnection.connection.prepareStatement(
                    "CREATE SCHEMA " + this.schemaName.toUpperCase() + " AUTHORIZATION DB2INST1");
            prepStat.executeUpdate();
            prepStat.close();
        }

        testConnection.connection.setSchema(this.schemaName.toUpperCase());
        testConnection.dbmsSchemaName = this.schemaName.toUpperCase();

        return testConnection;
    }
}