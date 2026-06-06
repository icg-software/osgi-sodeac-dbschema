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
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaColumnPropertiesIT extends AbstractDBSchemaIT
{
    
    public DBSchemaColumnPropertiesIT(final String dbType)
    {
        super(dbType);
    }
    
    private final String table1Name = "TableColChar";
    private final String columnIdName = "id";
    private final String columnCharName = "column_char";
    private final String columnNumberName = "column_number";
    
    @Test
    public void test001100CreateCharColumn() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
        
    }
    
    @Test
    public void test001101CreateCharColumnAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001102InsertFailed() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        
        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);
            
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnIdName + ") values (?)");
            prepStat.setString(1, UUID.randomUUID().toString());
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
            try
            {
                rset.close();
            }
            catch (final Exception e) { }
            try
            {
                prepStat.close();
            }
            catch (final Exception e) { }
        }
        
        fail("Expected an SQLException to be thrown");
    }
    
    @Test
    public void test001103SetDefaultToCharColumn() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        column1.setDefaultValue("'defaultvalue1'");
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001104SetDefaultToCharColumnAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        column1.setDefaultValue("'defaultvalue1'");
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001105InsertSuccess() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        
        PreparedStatement prepStat = null;
        ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);
            
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnIdName + ") values (?)");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
            
            int count = 0;
            prepStat = connection.prepareStatement("select " + this.columnCharName + " from " + this.table1Name);
            rset = prepStat.executeQuery();
            while (rset.next())
            {
                count++;
                assertEquals("value should be correct", "defaultvalue1", rset.getString(1));
            }
            rset.close();
            prepStat.close();
            
            assertEquals("rset should contains correct counts of entries", 1, count);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            connection.rollback();
            throw e;
        }
        finally
        {
            try
            {
                rset.close();
            }
            catch (final Exception e) { }
            try
            {
                prepStat.close();
            }
            catch (final Exception e) { }
        }
    }
    
    @Test
    public void test001106UnsetDefaultToCharColumn() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001107UnsetDefaultToCharColumnAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 21);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001108InsertFailedAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        
        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);
            
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnIdName + ") values (?)");
            prepStat.setString(1, UUID.randomUUID().toString());
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
            try
            {
                rset.close();
            }
            catch (final Exception e) { }
            try
            {
                prepStat.close();
            }
            catch (final Exception e) { }
        }
        
        fail("Expected an SQLException to be thrown");
    }
    
    @Test
    public void test001120InsertFailedLength() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        
        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);
            
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnIdName + "," + this.columnCharName + ") values (?,?)");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "aaaaaaaaaabbbbbbbbbbcccccccccc");
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
            try
            {
                rset.close();
            }
            catch (final Exception e) { }
            try
            {
                prepStat.close();
            }
            catch (final Exception e) { }
        }
        
        fail("Expected an SQLException to be thrown");
    }
    
    @Test
    public void test001121CharColumnNewLength() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 42);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_SIZE, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_SIZE, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001122CharColumnNewLengthAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnCharName, IColumnType.ColumnType.VARCHAR.toString(), false, 42);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
    }
    
    @Test
    public void test001123InsertSuccessLength() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        
        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);
            
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (" + this.columnIdName + "," + this.columnCharName + ") values (?,?)");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.setString(2, "aaaaaaaaaabbbbbbbbbbcccccccccc");
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();
            
        }
        catch (final Exception e)
        {
            connection.rollback();
            throw e;
        }
        finally
        {
            try
            {
                rset.close();
            }
            catch (final Exception e) { }
            try
            {
                prepStat.close();
            }
            catch (final Exception e) { }
        }
        
    }
    
    @Test
    public void test001130CreateNumberColumn() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnNumberName, IColumnType.ColumnType.SMALLINT.toString(), true);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
        
    }
    
    @Test
    public void test001131CreateNumberColumnAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnNumberName, IColumnType.ColumnType.SMALLINT.toString(), true);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
        
    }
    
    @Test
    public void test001133UpdateNumberColumn() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnNumberName, IColumnType.ColumnType.BIGINT.toString(), true);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_TYPE, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_TYPE, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
        
    }
    
    @Test
    public void test001134UpdateNumberColumnAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if (!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
        
        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
        
        ctrl.checkOrder(true);
        
        // create spec
        final SchemaSpec spec = new SchemaSpec(DOMAIN);
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
        
        final ColumnSpec columnPK = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPK.setPrimaryKey();
        
        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPK);
        
        final ColumnSpec column1 = table1.addColumn(this.columnNumberName, IColumnType.ColumnType.BIGINT.toString(), true);
        
        // prepare column for simulation
        
        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);
        
        // simulate listener
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        
        // table creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, DOMAIN, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, DOMAIN, table1Dictionary, driver, null);
        
        // table1 column creation
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1ColumnPKDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, DOMAIN, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, DOMAIN, table1Column1Dictionary, driver, null);
        
        // convert schema
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, DOMAIN, schemaDictionary, driver, null);
        
        ctrl.replay();
        
        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);
        
        ctrl.verify();
        
    }
}
