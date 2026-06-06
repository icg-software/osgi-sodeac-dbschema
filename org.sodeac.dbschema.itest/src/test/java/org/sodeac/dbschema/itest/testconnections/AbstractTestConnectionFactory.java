package org.sodeac.dbschema.itest.testconnections;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;

import org.sodeac.dbschema.itest.TestConnection;

public abstract class AbstractTestConnectionFactory implements Callable<TestConnection>, Serializable
{
    @Serial
    private static final long serialVersionUID = 1L;
    
    protected final Map<String, Boolean> createdSchema;
    protected final String schemaName;
    
    protected AbstractTestConnectionFactory(final Map<String, Boolean> createdSchema, final String schemaName)
    {
        this.createdSchema = createdSchema;
        this.schemaName = schemaName;
    }
}