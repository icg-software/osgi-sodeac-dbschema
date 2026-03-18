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

import java.io.IOException;
import java.sql.Connection;
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
public class DBSchemaColumn extends AbstractDBSchemaTest
{
    public DBSchemaColumn(final String dbType) { super(dbType); }

    @Test
    public void test000200CreateColumnUnquoted() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table1ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000201CreateColumnUnquotedAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table1ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000210CreateColumnQuotedTab() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table2ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);
        table1.setQuotedName(true);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000211CreateColumnQuotedTabAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table2ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);
        table1.setQuotedName(true);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000220CreateColumnQuotedCol() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table3ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("cOlumn1", IColumnType.ColumnType.CHAR.toString());
        column1.setQuotedName(true);
        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000221CreateColumnQuotedColAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table3ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("cOlumn1", IColumnType.ColumnType.CHAR.toString());
        column1.setQuotedName(true);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000230CreateColumnNotNullUnquoted() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table4ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());
        column1.setNullable(false);
        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        // table1 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000231CreateColumnNotNullUnquotedAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table4ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("column1", IColumnType.ColumnType.CHAR.toString());
        column1.setNullable(false);
        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000240CreateColumnNotNullQuoted() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }

        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table5ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("cOlumn1", IColumnType.ColumnType.CHAR.toString());
        column1.setNullable(false);
        column1.setQuotedName(true);
        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        // table1 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test000241CreateColumnNotNullUnquotedAgain() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;
        final String table1Name = "Table5ColumnA";

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        final SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(table1Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final ColumnSpec column1 = table1.addColumn("cOlumn1", IColumnType.ColumnType.CHAR.toString());
        column1.setNullable(false);
        column1.setQuotedName(true);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
        table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Column1Dictionary.put(ObjectType.TABLE, table1);
        table1Column1Dictionary.put(ObjectType.COLUMN, column1);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

}
