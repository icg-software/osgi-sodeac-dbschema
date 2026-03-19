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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.UUID;

import org.easymock.IMocksControl;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaColumnTypeTimeIT extends AbstractDBSchemaIT
{
    public DBSchemaColumnTypeTimeIT(final String dbType) { super(dbType); }

    private final String databaseID = "TESTDOMAIN";
    private final String table1Name = "TableColTime";
    private final String columnTimeName = "col_time";
    private final String columnDateName = "col_date";
    private final String columnTimestampName = "col_ts";

    private final String tableDfltName = "TableColDflt";
    private final String columnDfltTimeName = "col_time";
    private final String columnDfltDateName = "col_date";
    private final String columnDfltTimestampName = "col_ts";

    @Test
    public void test000600time() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnTimeName, IColumnType.ColumnType.TIME.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("select " + this.columnTimeName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            assertFalse("rset should contains no more entries", rset.next());
            rset.close();
            prepStat.close();

            final Time time = new Time(0, 12, 00);

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnTimeName + ") values (?)");
            prepStat.setTime(1, time);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimeName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", time.toString(), rset.getTime(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000601timeAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnTimeName, IColumnType.ColumnType.TIME.toString(), true);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final Time time = new Time(0, 12, 00);

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimeName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", time.toString(), rset.getTime(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000603TimeUpdate() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final Time time = new Time(0, 13, 00);

            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnTimeName + " = ? ");
            prepStat.setTime(1, time);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimeName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", time.toString(), rset.getTime(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);

        }
        catch (final SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000610date() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnDateName, IColumnType.ColumnType.DATE.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            final java.sql.Date date = new java.sql.Date(118, 0, 1);

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnDateName + " = ?");
            prepStat.setDate(1, date);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnDateName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getDate(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000611dateAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnDateName, IColumnType.ColumnType.DATE.toString(), true);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final java.sql.Date date = new java.sql.Date(118, 0, 1);

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnDateName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getDate(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000613DateUpdate() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final java.sql.Date date = new java.sql.Date(117, 0, 1);

            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnDateName + " = ? ");
            prepStat.setDate(1, date);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnDateName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getDate(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);

        }
        catch (final SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000620timestamp() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnTimestampName, IColumnType.ColumnType.TIMESTAMP.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            final java.sql.Timestamp date = new java.sql.Timestamp(118, 0, 1, 12, 0, 0, 0);

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnTimestampName + " = ?");
            prepStat.setTimestamp(1, date);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimestampName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getTimestamp(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000621TimestampAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnTimestampName, IColumnType.ColumnType.TIMESTAMP.toString(), true);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final java.sql.Timestamp date = new java.sql.Timestamp(118, 0, 1, 12, 0, 0, 0);

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimestampName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getTimestamp(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000623TimestampUpdate() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            final java.sql.Timestamp date = new java.sql.Timestamp(115, 0, 1, 17, 0, 0, 0);

            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnTimestampName + " = ? ");
            prepStat.setTimestamp(1, date);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnTimestampName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", date.toString(), rset.getTimestamp(1).toString());
            }
            rset.close();
            prepStat.close();

            assertEquals("rset should contains correct counts of entries", 1, count);

        }
        catch (final SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000630timedefaults() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        final TableSpec table1 = spec.addTable(this.tableDfltName);

        table1.addColumn("id", IColumnType.ColumnType.CHAR.toString(), false, 36);
        table1.addColumn(this.columnDfltTimeName, IColumnType.ColumnType.TIME.toString(), false).setDefaultValue(IDatabaseSchemaDriver.Function.CURRENT_TIME.toString()).setDefaultValueByFunction(true);
        table1.addColumn(this.columnDfltDateName, IColumnType.ColumnType.DATE.toString(), false).setDefaultValue(IDatabaseSchemaDriver.Function.CURRENT_DATE.toString()).setDefaultValueByFunction(true);
        table1.addColumn(this.columnDfltTimestampName, IColumnType.ColumnType.TIMESTAMP.toString(), false).setDefaultValue(IDatabaseSchemaDriver.Function.CURRENT_TIMESTAMP.toString()).setDefaultValueByFunction(true);

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("insert into " + this.tableDfltName + " (id) values (?)");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Configuration
    public static Option[] config()
    {
        return Statics.config();
    }
}
