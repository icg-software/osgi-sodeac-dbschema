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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.easymock.IMocksControl;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
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
public class DBSchemaColumnTypeTextIT extends AbstractDBSchemaIT
{

    public DBSchemaColumnTypeTextIT(final String dbType) { super(dbType); }

    private final String databaseID = "TESTDOMAIN";
    private final String table1Name = "TableColChar";
    private final String columnCharName = "column_char";
    private final String columnVarcharName = "column_varchar";
    private final String columnClobName = "column_clob";

    @Test
    public void test000300char() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.CHAR.toString(), true, 100);

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

            prepStat = connection.prepareStatement("select " + this.columnCharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            assertFalse("rset should contains no more entries", rset.next());
            rset.close();
            prepStat.close();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnCharName + ") values (?)");
            prepStat.setString(1, "valueforcolumn");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnCharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "valueforcolumn", rset.getString(1).trim());
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
    public void test000301charAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.CHAR.toString(), true, 100);

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

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnCharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "valueforcolumn", rset.getString(1).trim());
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
    public void test000303charToLong() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnCharName + ") values (?)");
            prepStat.setString(1, "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

        }
        catch (final SQLException e)
        {
            return;
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
        fail("Expected an SQLException to be thrown");
    }

    @Test
    public void test000310varchar() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnVarcharName, IColumnType.ColumnType.VARCHAR.toString(), true, 100);

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

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnVarcharName + " = ? ");
            prepStat.setString(1, "value2forcolumn");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnVarcharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "value2forcolumn", rset.getString(1));
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
    public void test000311varcharAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnVarcharName, IColumnType.ColumnType.VARCHAR.toString(), true, 100);

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

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnVarcharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "value2forcolumn", rset.getString(1));
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
    public void test000313varcharToLong() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnVarcharName + ") values (?)");
            prepStat.setString(1, "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

        }
        catch (final SQLException e)
        {
            return;
        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
        fail("Expected an SQLException to be thrown");
    }

    @Test
    public void test000320clob() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnClobName, IColumnType.ColumnType.CLOB.toString(), true);

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

            prepStat = connection.prepareStatement("update " + this.table1Name + " set " + this.columnClobName + " = ? ");
            prepStat.setString(1, "valuelobforcolumn");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnClobName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "valuelobforcolumn", rset.getString(1));
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
    public void test000321clobAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnClobName, IColumnType.ColumnType.CLOB.toString(), true);

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

            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnClobName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "valuelobforcolumn", rset.getString(1));
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
    public void test000323clobLong() throws SQLException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnClobName + ") values (?)");
            prepStat.setString
                            (
                                    1,
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn" +
                                    "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn"
                            );
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
}
