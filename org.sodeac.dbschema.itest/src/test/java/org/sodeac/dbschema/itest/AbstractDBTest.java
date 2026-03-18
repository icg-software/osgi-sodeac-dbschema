package org.sodeac.dbschema.itest;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.easymock.EasyMockSupport;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaProcessor;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.itest.testconnections.EDbType;

@ExamReactorStrategy(PerSuite.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractDBTest
{
    protected static final String DOMAIN = "TESTDOMAIN";

    protected static final Map<String, Boolean> createdSchema = new ConcurrentHashMap<>();

    @Configuration
    public static Option[] config()
    {
        return Statics.config();
    }

    protected final EasyMockSupport support = new EasyMockSupport();

    protected TestConnection testConnection;

    @Inject
    protected IDatabaseSchemaProcessor databaseSchemaProcessor;

    protected final EDbType dbType;

    protected AbstractDBTest(final String dbType) { this.dbType = EDbType.valueOf(dbType); }

    @Before
    public void setUp() throws Exception
    {
        System.out.println("type: " + this.dbType);
        this.testConnection = Statics.createConnection(this.dbType, createdSchema);
        System.out.println("conn: " + this.testConnection);
    }

    @After
    public void tearDown()
    {
        if(!this.testConnection.enabled) { return; }

        if(this.testConnection.connection != null)
        {
            try
            {
                this.testConnection.connection.close();
            }
            catch (final Exception e) { }
        }
    }

    protected IDatabaseSchemaDriver driver() throws SQLException
    {
        return this.databaseSchemaProcessor.getDatabaseSchemaDriver(this.testConnection.connection);
    }

    protected IMocksControl newControl()
    {
        return this.support.createControl();
    }

    protected SchemaSpec newSchemaSpec()
    {
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);
        return spec;
    }
}