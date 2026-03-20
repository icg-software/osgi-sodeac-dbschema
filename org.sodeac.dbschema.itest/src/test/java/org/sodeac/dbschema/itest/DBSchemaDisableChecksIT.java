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
import org.sodeac.dbschema.api.IndexSpec;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaDisableChecksIT extends AbstractDBSchemaIT
{
    public DBSchemaDisableChecksIT(final String dbType) { super(dbType); }

    private final String databaseID = "TESTDOMAIN";
    private final String table1Name = "TableDisableCheck1";
    private final String table2Name = "TableDisableCheck2";

    private final String columnIdName = "id";
    private final String columnFKName = "fk";
    private final String columnUniqueName = "unq1";

    @Test
    public void test001200generateWithDisabledChecks() throws SQLException, ClassNotFoundException, IOException
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
        spec.setSkipChecks(true);

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

        final ColumnSpec columnPkTable1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable1.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);

        final ColumnSpec columnFKTable1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        columnFKTable1.setForeignKey("fk1_tbl_dis_check", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
        table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);

        final ColumnSpec columnUnqTable1 = table1.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
        table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
        table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);

        table1.addColumnIndex("unq1_tbl1_dis_check", this.columnUniqueName, true);

        final TableSpec table2 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
        table2Dictionary.put(ObjectType.SCHEMA, spec);
        table2Dictionary.put(ObjectType.TABLE, table2);
        table2.addUpdateListener(updateListenerMock);

        final ColumnSpec columnPkTable2 = table2.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable2.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
        table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
        table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);

        final ColumnSpec columnUnqTable2 = table2.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
        table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
        table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);

        table2.addColumnIndex("unq1_tbl2_dis_check", this.columnUniqueName, true);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);

        // table2 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);

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
    public void test001201generateWithDisabledChecksAgain() throws SQLException, ClassNotFoundException, IOException
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
        spec.setSkipChecks(true);

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

        final ColumnSpec columnPkTable1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable1.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);

        final ColumnSpec columnFKTable1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        columnFKTable1.setForeignKey("fk1_tbl_dis_check", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
        table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);

        final ColumnSpec columnUnqTable1 = table1.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
        table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
        table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);

        table1.addColumnIndex("unq1_tbl1_dis_check", this.columnUniqueName, true);

        final TableSpec table2 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
        table2Dictionary.put(ObjectType.SCHEMA, spec);
        table2Dictionary.put(ObjectType.TABLE, table2);
        table2.addUpdateListener(updateListenerMock);

        final ColumnSpec columnPkTable2 = table2.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable2.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
        table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
        table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);

        final ColumnSpec columnUnqTable2 = table2.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
        table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
        table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);

        table2.addColumnIndex("unq1_tbl2_dis_check", this.columnUniqueName, true);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);

        // table2 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);

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
    public void test001250generateWithEnabledChecks() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec columnPkTable1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable1.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);

        final ColumnSpec columnFKTable1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        columnFKTable1.setForeignKey("fk1_tbl_dis_check", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
        table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);

        final ColumnSpec columnUnqTable1 = table1.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
        table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
        table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);

        final IndexSpec index1Table1 = table1.addColumnIndex("unq1_tbl1_dis_check", this.columnUniqueName, true);

        final Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
        table1index1Dictionary.put(ObjectType.SCHEMA, spec);
        table1index1Dictionary.put(ObjectType.TABLE, table1);
        table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1Table1);

        final TableSpec table2 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
        table2Dictionary.put(ObjectType.SCHEMA, spec);
        table2Dictionary.put(ObjectType.TABLE, table2);
        table2.addUpdateListener(updateListenerMock);

        final ColumnSpec columnPkTable2 = table2.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable2.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
        table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
        table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);

        final ColumnSpec columnUnqTable2 = table2.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
        table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
        table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);

        final IndexSpec index1Table2 = table2.addColumnIndex("unq1_tbl2_dis_check", this.columnUniqueName, true);

        final Dictionary<ObjectType, Object> table2index1Dictionary = new Hashtable<>();
        table2index1Dictionary.put(ObjectType.SCHEMA, spec);
        table2index1Dictionary.put(ObjectType.TABLE, table2);
        table2index1Dictionary.put(ObjectType.TABLE_INDEX, index1Table2);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);

        // table2 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        // table1 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);

        // table2 column properties

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);

        // table1 create keys/indices

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, this.databaseID, table1index1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, this.databaseID, table1index1Dictionary, driver, null);

        // table2 create keys/indices

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, this.databaseID, table2index1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, this.databaseID, table2index1Dictionary, driver, null);

        // table1 column foreign keys

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, this.databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test001251generateWithEnabledChecksAgain() throws SQLException, ClassNotFoundException, IOException
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

        final ColumnSpec columnPkTable1 = table1.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable1.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);

        final ColumnSpec columnFKTable1 = table1.addColumn(this.columnFKName, IColumnType.ColumnType.CHAR.toString(), true, 36);
        columnFKTable1.setForeignKey("fk1_tbl_dis_check", this.table2Name, this.columnIdName);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
        table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);

        final ColumnSpec columnUnqTable1 = table1.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
        table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
        table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);

        table1.addColumnIndex("unq1_tbl1_dis_check", this.columnUniqueName, true);

        final TableSpec table2 = spec.addTable(this.table2Name);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
        table2Dictionary.put(ObjectType.SCHEMA, spec);
        table2Dictionary.put(ObjectType.TABLE, table2);
        table2.addUpdateListener(updateListenerMock);

        final ColumnSpec columnPkTable2 = table2.addColumn(this.columnIdName, IColumnType.ColumnType.CHAR.toString(), false, 36);
        columnPkTable2.setPrimaryKey();

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
        table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
        table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);

        final ColumnSpec columnUnqTable2 = table2.addColumn(this.columnUniqueName, IColumnType.ColumnType.CHAR.toString(), true, 36);

        // prepare column for simulation

        final Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
        table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
        table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
        table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);

        table2.addColumnIndex("unq1_tbl2_dis_check", this.columnUniqueName, true);

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, this.databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, this.databaseID, table2Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, this.databaseID, table2Dictionary, driver, null);

        // table1 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnFKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnFKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table1ColumnUnqDictionary, driver, null);

        // table2 column creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, this.databaseID, table2ColumnUnqDictionary, driver, null);

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
