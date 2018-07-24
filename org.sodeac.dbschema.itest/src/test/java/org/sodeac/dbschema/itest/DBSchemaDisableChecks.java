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

import org.easymock.EasyMockSupport;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized.Parameters;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExamParameterized;

import static org.junit.Assert.fail;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaProcessor;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.IndexSpec;
import org.sodeac.dbschema.api.ColumnSpec;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.inject.Inject;


@RunWith(PaxExamParameterized.class)
@ExamReactorStrategy(PerSuite.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaDisableChecks
{
	private EasyMockSupport support = new EasyMockSupport();
	
	public static List<Object[]> connectionList = null;
	public static final Map<String,Boolean> createdSchema = new HashMap<String,Boolean>();
	
	@Inject
	private IDatabaseSchemaProcessor databaseSchemaProcessor;
	
	private String databaseID = "TESTDOMAIN";
	private String table1Name = "TableDisableCheck1";
	private String table2Name = "TableDisableCheck2";
	
	private String columnIdName = "id";
	private String columnFKName = "fk";
	private String columnUniqueName = "unq1";

	@Parameters
    public static List<Object[]> connections()
    {
    	if(connectionList != null)
    	{
    		return connectionList;
    	}
    	return connectionList = Statics.connections(createdSchema);
    }
	
	public DBSchemaDisableChecks(Callable<TestConnection> connectionFactory)
	{
		this.testConnectionFactory = connectionFactory;
	}
	
	Callable<TestConnection> testConnectionFactory = null;
	TestConnection testConnection = null;
	
	@Before
	public void setUp() throws Exception 
	{
		this.testConnection = testConnectionFactory.call();
	}
	
	@After
	public void tearDown()
	{
		if(! this.testConnection.enabled)
		{
			return;
		}
		if(this.testConnection.connection != null)
		{
			try
			{
				this.testConnection.connection.close();
			}
			catch (Exception e) {}
		}
	}
	
	
	@Test
	public void test001200generateWithDisabledChecks() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		spec.setSkipChecks(true);
		
		// prepare spec for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
		
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable1 = table1.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable1.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
		table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);
		
		ColumnSpec columnFKTable1 = table1.addColumn(columnFKName, IColumnType.ColumnType.CHAR.toString(),true,36);
		columnFKTable1.setForeignKey("fk1_tbl_dis_check", table2Name,columnIdName);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
		table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);
		
		ColumnSpec columnUnqTable1 = table1.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
		table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
		table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);
		
		table1.addColumnIndex("unq1_tbl1_dis_check", columnUniqueName, true);
		
		TableSpec table2 = spec.addTable(table2Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
		table2Dictionary.put(ObjectType.SCHEMA, spec);
		table2Dictionary.put(ObjectType.TABLE, table2);
		table2.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable2 = table2.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable2.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
		table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
		table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);
		
		
		ColumnSpec columnUnqTable2 = table2.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
		table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
		table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);
		
		table2.addColumnIndex("unq1_tbl2_dis_check", columnUniqueName, true);
		
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
		
		// table1 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		
		// table2 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnUnqDictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
	}
	
	@Test
	public void test001201generateWithDisabledChecksAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		spec.setSkipChecks(true);
		
		// prepare spec for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
		
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable1 = table1.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable1.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
		table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);
		
		ColumnSpec columnFKTable1 = table1.addColumn(columnFKName, IColumnType.ColumnType.CHAR.toString(),true,36);
		columnFKTable1.setForeignKey("fk1_tbl_dis_check", table2Name,columnIdName);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
		table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);
		
		ColumnSpec columnUnqTable1 = table1.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
		table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
		table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);
		
		table1.addColumnIndex("unq1_tbl1_dis_check", columnUniqueName, true);
		
		TableSpec table2 = spec.addTable(table2Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
		table2Dictionary.put(ObjectType.SCHEMA, spec);
		table2Dictionary.put(ObjectType.TABLE, table2);
		table2.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable2 = table2.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable2.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
		table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
		table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);
		
		
		ColumnSpec columnUnqTable2 = table2.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
		table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
		table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);
		
		table2.addColumnIndex("unq1_tbl2_dis_check", columnUniqueName, true);
		
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
		
		// table1 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		
		// table2 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnUnqDictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
	}
	
	@Test
	public void test001250generateWithEnabledChecks() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		
		// prepare spec for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
		
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable1 = table1.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable1.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
		table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);
		
		ColumnSpec columnFKTable1 = table1.addColumn(columnFKName, IColumnType.ColumnType.CHAR.toString(),true,36);
		columnFKTable1.setForeignKey("fk1_tbl_dis_check", table2Name,columnIdName);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
		table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);
		
		ColumnSpec columnUnqTable1 = table1.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
		table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
		table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);
		
		IndexSpec index1Table1 = table1.addColumnIndex("unq1_tbl1_dis_check", columnUniqueName, true);
		
		Dictionary<ObjectType, Object> table1index1Dictionary = new Hashtable<>();
		table1index1Dictionary.put(ObjectType.SCHEMA, spec);
		table1index1Dictionary.put(ObjectType.TABLE, table1);
		table1index1Dictionary.put(ObjectType.TABLE_INDEX, index1Table1);
		
		TableSpec table2 = spec.addTable(table2Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
		table2Dictionary.put(ObjectType.SCHEMA, spec);
		table2Dictionary.put(ObjectType.TABLE, table2);
		table2.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable2 = table2.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable2.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
		table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
		table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);
		
		
		ColumnSpec columnUnqTable2 = table2.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
		table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
		table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);
		
		IndexSpec index1Table2 = table2.addColumnIndex("unq1_tbl2_dis_check", columnUniqueName, true);
		
		Dictionary<ObjectType, Object> table2index1Dictionary = new Hashtable<>();
		table2index1Dictionary.put(ObjectType.SCHEMA, spec);
		table2index1Dictionary.put(ObjectType.TABLE, table2);
		table2index1Dictionary.put(ObjectType.TABLE_INDEX, index1Table2);
		
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
		
		// table1 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		
		// table2 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnUnqDictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);

		// table1 column properties
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		
		// table2 column properties
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
					
		// table1 create keys/indices
					
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, databaseID, table1index1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, databaseID, table1index1Dictionary, driver, null);
		
		// table2 create keys/indices
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, databaseID, table2index1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, databaseID, table2index1Dictionary, driver, null);
		
		// table1 column foreign keys
		
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
		
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
	}
	
	@Test
	public void test001251generateWithEnabledChecksAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		
		// prepare spec for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
		
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable1 = table1.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable1.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
		table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable1);
		
		ColumnSpec columnFKTable1 = table1.addColumn(columnFKName, IColumnType.ColumnType.CHAR.toString(),true,36);
		columnFKTable1.setForeignKey("fk1_tbl_dis_check", table2Name,columnIdName);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnFKDictionary = new Hashtable<>();
		table1ColumnFKDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnFKDictionary.put(ObjectType.TABLE, table1);
		table1ColumnFKDictionary.put(ObjectType.COLUMN, columnFKTable1);
		
		ColumnSpec columnUnqTable1 = table1.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1ColumnUnqDictionary = new Hashtable<>();
		table1ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table1ColumnUnqDictionary.put(ObjectType.TABLE, table1);
		table1ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable1);
		
		table1.addColumnIndex("unq1_tbl1_dis_check", columnUniqueName, true);
		
		TableSpec table2 = spec.addTable(table2Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
		table2Dictionary.put(ObjectType.SCHEMA, spec);
		table2Dictionary.put(ObjectType.TABLE, table2);
		table2.addUpdateListener(updateListenerMock);
		
		ColumnSpec columnPkTable2 = table2.addColumn(columnIdName, IColumnType.ColumnType.CHAR.toString(),false,36);
		columnPkTable2.setPrimaryKey();
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnPKDictionary = new Hashtable<>();
		table2ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnPKDictionary.put(ObjectType.TABLE, table2);
		table2ColumnPKDictionary.put(ObjectType.COLUMN, columnPkTable2);
		
		
		ColumnSpec columnUnqTable2 = table2.addColumn(columnUniqueName, IColumnType.ColumnType.CHAR.toString(),true,36);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table2ColumnUnqDictionary = new Hashtable<>();
		table2ColumnUnqDictionary.put(ObjectType.SCHEMA, spec);
		table2ColumnUnqDictionary.put(ObjectType.TABLE, table2);
		table2ColumnUnqDictionary.put(ObjectType.COLUMN, columnUnqTable2);
		
		table2.addColumnIndex("unq1_tbl2_dis_check", columnUniqueName, true);
		
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
		
		// table1 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnFKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnFKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnUnqDictionary, driver, null);
		
		// table2 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnPKDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnPKDictionary, driver, null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2ColumnUnqDictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2ColumnUnqDictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
	}
	
	@Configuration
	public static Option[] config() 
	{
		return Statics.config();
	}
}
