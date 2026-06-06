package org.sodeac.dbschema.itest.testconnections;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.TestConnection;

public final class MySqlTestConnectionFactory
{
    private MySqlTestConnectionFactory() { }
    
    public static TestConnection create(final Map<String, Boolean> createdSchema, final String schemaName, final boolean isEnabled)
        throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(isEnabled);
        if (!testConnection.enabled)
        {
            return testConnection;
        }
        
        // docker run --name=mysql -e MYSQL_ROOT_HOST=% -e MYSQL_ROOT_PASSWORD=sodeac -p 3306:3306 -d mysql/mysql-server
        //
        // CREATE TABLESPACE sodeacdata ADD DATAFILE 'sodeacdata.ibd' ENGINE=INNODB
        // CREATE TABLESPACE sodeacindex ADD DATAFILE 'sodeacindex.ibd' ENGINE=INNODB
        
        try
        {
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        }
        catch (final Exception e) { }
        
        testConnection.connection =
            DriverManager.getConnection("jdbc:mysql://127.0.0.1/?useSSL=false", "root", "sodeac");
        
        if (createdSchema.get("MYSQL_" + schemaName) == null)
        {
            createdSchema.put("MYSQL_" + schemaName, true);
            
            final PreparedStatement prepStat = testConnection.connection.prepareStatement(
                "CREATE SCHEMA " + schemaName.toLowerCase() + " CHARACTER SET = utf8 COLLATE = utf8_general_ci");
            prepStat.executeUpdate();
            prepStat.close();
        }
        testConnection.connection.setSchema(schemaName.toLowerCase());
        testConnection.connection.setCatalog(schemaName.toLowerCase());
        testConnection.dbmsSchemaName = schemaName.toLowerCase();
        
        return testConnection;
    }
}