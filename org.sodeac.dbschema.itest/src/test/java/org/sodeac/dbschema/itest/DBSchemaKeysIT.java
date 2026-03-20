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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.UUID;

import org.easymock.IMocksControl;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.IndexSpec;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaKeysIT extends AbstractDBSchemaIT
{
    public DBSchemaKeysIT(final String dbType) { super(dbType); }

    private final String databaseID = "TESTDOMAIN";
    private final String table1Name = "TableKeys1";
    private final String table2Name = "TableKeys2";

    private final String columnIdName = "id";
    private final String columnFKName = "fk";
    private final String columnFK2Name = "fk2";
    private final String columnIdx1Name = "idx1";
    private final String columnIdx2Name = "idx2";
    private final String columnIdx3Name = "idx3";
    private final String columnIdx4Name = "idx4";

    @Test
    public void test001000primaryKey() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        column1.setPrimaryKey();
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

        // table1 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // table1 create keys/indices

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001001primaryKeyAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        column1.setPrimaryKey();
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

    }

    @Test
    public void test001002PrimaryKeyInsertSuccess() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + ")  values (?) ");
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

    @Test
    public void test00103PrimaryKeyInsertFailure() throws SQLException, ClassNotFoundException, IOException
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

            final String uuid = UUID.randomUUID().toString();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
        }
        catch (final Exception e)
        {
            connection.rollback();
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
    public void test001010primaryKeyTS() throws SQLException, ClassNotFoundException, IOException
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

        final TableSpec table1 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        column1.setPrimaryKey(null, null, false, "sodeacindex");
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

        // table1 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        // table1 create keys/indices

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001011primaryKeyAgainTS() throws SQLException, ClassNotFoundException, IOException
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

        final TableSpec table1 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        column1.setPrimaryKey(null, null, false, "sodeacindex");

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

    }

    @Test
    public void test001012PrimaryKeyInsertSuccessTS() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table2Name + "  (" + this.columnIdName + ")  values (?) ");
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

    @Test
    public void test001013PrimaryKeyInsertFailureTS() throws SQLException, ClassNotFoundException, IOException
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

            final String uuid = UUID.randomUUID().toString();

            prepStat = connection.prepareStatement("insert into " + this.table2Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table2Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
        }
        catch (final Exception e)
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
    public void test001020foreignKey() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_xxx", this.table2Name, this.columnIdName);

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

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001021foreignKeyAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_xxx", this.table2Name, this.columnIdName);

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

    }

    @Test
    public void test001022foreignKeyInsertFailure() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnFKName + ")  values (?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
        }
        catch (final Exception e)
        {
            connection.rollback();
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
    public void test001023foreignKeyInsertSuccess() throws SQLException, ClassNotFoundException, IOException
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

            final String uuid = UUID.randomUUID().toString();

            prepStat = connection.prepareStatement("insert into " + this.table2Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnFKName + ")  values (?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, uuid);
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

    @Test
    public void test001024foreignKeyWithUsedName() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFK2Name, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_xxx", this.table2Name, this.columnIdName);

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

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001025foreignKeyInsertFailure() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnFK2Name + ")  values (?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
        }
        catch (final Exception e)
        {
            connection.rollback();
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
    public void test001026foreignKey2InsertSuccess() throws SQLException, ClassNotFoundException, IOException
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

            final String uuid = UUID.randomUUID().toString();

            prepStat = connection.prepareStatement("insert into " + this.table2Name + "  (" + this.columnIdName + ")  values (?) ");
            prepStat.setString(1, uuid);
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnFK2Name + ")  values (?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, uuid);
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

    @Test
    public void test001027ReForeignKey() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_re_xxx", this.table2Name, this.columnIdName);

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

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001028DropForeignKey() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFK2Name, IColumnType.ColumnType.CHAR.toString(), true, 36);

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

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001029ForeignKey2InsertSuccessAfterDrop() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnFK2Name + ")  values (?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
        }
        finally
        {
            connection.rollback();
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test001040index() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdx1Name, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final ColumnSpec column2 = table1.addColumn(this.columnIdx2Name, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column2Dictionary = new Hashtable<>();
        table1Column2Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column2Dictionary.put(ObjectType.TABLE, table1);
        table1Column2Dictionary.put(ObjectType.COLUMN, column2);

        final IndexSpec index1 = table1.addColumnIndex("idx1_" + table1.getName(), new String[] { column1.getName(), column2.getName() }, false);

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

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

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, this.databaseID, table1index1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, this.databaseID, table1index1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001041indexAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdx1Name, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final ColumnSpec column2 = table1.addColumn(this.columnIdx2Name, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column2Dictionary = new Hashtable<>();
        table1Column2Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column2Dictionary.put(ObjectType.TABLE, table1);
        table1Column2Dictionary.put(ObjectType.COLUMN, column2);

        final IndexSpec index1 = table1.addColumnIndex("idx1_" + table1.getName(), new String[] { column1.getName(), column2.getName() }, false);

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001043UniqueIndex() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdx3Name, IColumnType.ColumnType.VARCHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final ColumnSpec column2 = table1.addColumn(this.columnIdx4Name, IColumnType.ColumnType.VARCHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column2Dictionary = new Hashtable<>();
        table1Column2Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column2Dictionary.put(ObjectType.TABLE, table1);
        table1Column2Dictionary.put(ObjectType.COLUMN, column2);

        final IndexSpec index1 = table1.addColumnIndex("idx2_" + table1.getName(), new String[] { column1.getName(), column2.getName() }, true);

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

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

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, this.databaseID, table1index1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, this.databaseID, table1index1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001044uniqueIndexAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnIdx3Name, IColumnType.ColumnType.VARCHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final ColumnSpec column2 = table1.addColumn(this.columnIdx4Name, IColumnType.ColumnType.VARCHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column2Dictionary = new Hashtable<>();
        table1Column2Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column2Dictionary.put(ObjectType.TABLE, table1);
        table1Column2Dictionary.put(ObjectType.COLUMN, column2);

        final IndexSpec index1 = table1.addColumnIndex("idx2_" + table1.getName(), new String[] { column1.getName(), column2.getName() }, true);

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1Column2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1Column2Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001045IndiziesInsertSuccess() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx1Name + "," + this.columnIdx2Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "a");
            prepStat.setString(3, "b");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx1Name + "," + this.columnIdx2Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "a");
            prepStat.setString(3, "b");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx3Name + "," + this.columnIdx4Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "a");
            prepStat.setString(3, "b");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx3Name + "," + this.columnIdx4Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "a");
            prepStat.setString(3, "c");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx3Name + "," + this.columnIdx4Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "b");
            prepStat.setString(3, "c");
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

    @Test
    public void test001046IndiziesInsertFailure() throws SQLException, ClassNotFoundException, IOException
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

            prepStat = connection.prepareStatement("insert into " + this.table1Name + "  (" + this.columnIdName + "," + this.columnIdx3Name + "," + this.columnIdx4Name + ")  values (?,?,?) ");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "b");
            prepStat.setString(3, "c");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

        }
        catch (final Exception e)
        {
            connection.rollback();
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
    public void test001047IndexWithTablespace() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_re_xxx", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final IndexSpec index1 = table1.addColumnIndex("idx3_" + table1.getName(), new String[] { column1.getName() }, false).setTableSpace("sodeacindex");

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

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

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, this.databaseID, table1index1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, this.databaseID, table1index1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();

    }

    @Test
    public void test001047IndexWithTablespaceAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec column1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        column1.setForeignKey("fk1_re_xxx", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        final IndexSpec index1 = table1.addColumnIndex("idx3_" + table1.getName(), new String[] { column1.getName() }, false).setTableSpace("sodeacindex");

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1);

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

    }
}
