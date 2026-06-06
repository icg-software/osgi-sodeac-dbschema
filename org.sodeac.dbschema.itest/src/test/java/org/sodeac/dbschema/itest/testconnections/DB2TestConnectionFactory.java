package org.sodeac.dbschema.itest.testconnections;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.TestConnection;

public final class DB2TestConnectionFactory
{
    
    private DB2TestConnectionFactory() { }
    
    public static TestConnection create(final Map<String, Boolean> createdSchema, final String schemaName, final boolean isEnabled)
        throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(isEnabled);
        if (!testConnection.enabled)
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
        
        if (createdSchema.get("DB2_" + schemaName) == null)
        {
            createdSchema.put("DB2_" + schemaName, true);
            
            final PreparedStatement prepStat = testConnection.connection.prepareStatement(
                "CREATE SCHEMA " + schemaName.toUpperCase() + " AUTHORIZATION DB2INST1");
            prepStat.executeUpdate();
            prepStat.close();
        }
        
        testConnection.connection.setSchema(schemaName.toUpperCase());
        testConnection.dbmsSchemaName = schemaName.toUpperCase();
        
        return testConnection;
    }
}