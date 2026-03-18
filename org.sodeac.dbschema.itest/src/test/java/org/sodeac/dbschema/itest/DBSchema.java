/*******************************************************************************
 * Copyright (c) 2017, 2018 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.dbschema.itest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExamParameterized;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.itest.testconnections.EDbType;

@RunWith(PaxExamParameterized.class)
public class DBSchema extends AbstractDBTest
{
    public static final String SCHEMA_NAME = "SODEAC_TEST";

    public DBSchema(final String dbType) { super(dbType); }

    @Parameters(name = "{0}")
    public static List<Object[]> connections()
    {
        return Arrays.stream(EDbType.values())
                     // needs String >> enum.name()
                     .map(dbType -> new Object[] { dbType.name() })
                     .toList();
    }

    @Test
    public void test000001createSchema() throws SQLException, ClassNotFoundException, IOException
    {
        System.out.println("+++++++++++++++++++++++++++++++++++++");
        System.out.println(createdSchema);
        System.out.println(this.testConnection.enabled);
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final Map<String, Object> confirmMap = new HashMap<String, Object>();
        confirmMap.put("YES_I_REALLY_WANT_DROP_SCHEMA_" + SCHEMA_NAME.toUpperCase(), true);
        confirmMap.put("OF_COURSE_I_HAVE_A_BACKUP_OF_ALL_IMPORTANT_DATASETS", true);

        if(driver.schemaExists(connection, SCHEMA_NAME))
        {
            driver.dropSchema(connection, SCHEMA_NAME, confirmMap);
        }

        assertFalse("test schema should not exist", driver.schemaExists(connection, SCHEMA_NAME));

        driver.createSchema(connection, SCHEMA_NAME, null);
        assertTrue("test schema should exist", driver.schemaExists(connection, SCHEMA_NAME));

        driver.dropSchema(connection, SCHEMA_NAME, confirmMap);
        assertFalse("test schema should not exist", driver.schemaExists(connection, SCHEMA_NAME));
    }

    @Configuration
    public static Option[] config()
    {
        return Statics.config();
    }
}
