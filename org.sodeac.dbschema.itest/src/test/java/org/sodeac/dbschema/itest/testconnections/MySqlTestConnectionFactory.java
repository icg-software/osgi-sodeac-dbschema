package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.sodeac.dbschema.itest.Statics;
import org.sodeac.dbschema.itest.TestConnection;

public class MySqlTestConnectionFactory extends AbstractTestConnectionFactory
{
    @Serial
    private static final long serialVersionUID = 1L;

    public MySqlTestConnectionFactory(final Map<String, Boolean> createdSchema, final String schemaName)
    {
        super(createdSchema, schemaName);
    }

    @Override
    public TestConnection call() throws ClassNotFoundException, SQLException
    {
        final TestConnection testConnection = new TestConnection(Statics.ENABLED_MYSQL);
        if(!testConnection.enabled)
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

        if(this.createdSchema.get("MYSQL_" + this.schemaName) == null)
        {
            this.createdSchema.put("MYSQL_" + this.schemaName, true);

            final PreparedStatement prepStat = testConnection.connection.prepareStatement(
                    "CREATE SCHEMA " + this.schemaName.toLowerCase() + " CHARACTER SET = utf8 COLLATE = utf8_general_ci");
            prepStat.executeUpdate();
            prepStat.close();
        }
        testConnection.connection.setSchema(this.schemaName.toLowerCase());
        testConnection.connection.setCatalog(this.schemaName.toLowerCase());
        testConnection.dbmsSchemaName = this.schemaName.toLowerCase();

        return testConnection;
    }
}