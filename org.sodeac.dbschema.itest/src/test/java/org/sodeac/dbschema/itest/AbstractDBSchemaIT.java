package org.sodeac.dbschema.itest;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

import org.easymock.EasyMockSupport;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExamParameterized;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaProcessor;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.itest.testconnections.EDbType;

@RunWith(PaxExamParameterized.class)
@ExamReactorStrategy(PerSuite.class)
public abstract class AbstractDBSchemaIT
{
    protected static final String DOMAIN = "TESTDOMAIN";

    protected static final Map<String, Boolean> createdSchema = new ConcurrentHashMap<>();

    @Configuration
    public static Option[] config()
    {
        return Statics.config();
    }

    @Parameters(name = "{0}")
    public static List<Object[]> connections()
    {
        return Arrays.stream(EDbType.values())
                     // needs String >> enum.name()
                     .map(dbType -> new Object[] { dbType.name() })
                     .toList();
    }

    protected final EasyMockSupport support = new EasyMockSupport();

    protected TestConnection testConnection;

    @Inject
    protected IDatabaseSchemaProcessor databaseSchemaProcessor;

    protected final EDbType dbType;

    protected AbstractDBSchemaIT(final String dbType) { this.dbType = EDbType.valueOf(dbType); }

    @Before
    public void setUp() throws Exception
    {
        // FIXME: sysouts raus
        // FIXME: connectionfactories ohne Callable
        System.out.println("type: " + this.dbType);
        this.testConnection = Statics.createConnection(this.dbType, createdSchema, this.getClass().getSimpleName());
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