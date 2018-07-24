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
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaProcessor;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.api.ActionType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;


@RunWith(PaxExamParameterized.class)
@ExamReactorStrategy(PerSuite.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaTable
{
	
	
	public static final String DOMAIN = "TESTDOMAIN";
	
	private EasyMockSupport support = new EasyMockSupport();
	
	public static List<Object[]> connectionList = null;
	public static final Map<String,Boolean> createdSchema = new HashMap<String,Boolean>();
	
	@Inject
	private IDatabaseSchemaProcessor databaseSchemaProcessor;
	
	@Parameters
    public static List<Object[]> connections()
    {
    	if(connectionList != null)
    	{
    		return connectionList;
    	}
    	return connectionList = Statics.connections(createdSchema);
    }
	
	
	public DBSchemaTable(Callable<TestConnection> connectionFactory)
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
	public void test000101CreateTableUnquoted() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTable1";
		
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
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000102CreateTableUnquotedAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTable1";
		
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
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000111CreateTableQuoted() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTableQ1";
		
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
		table1.setQuotedName(true);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000112CreateTableQuotedAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTableQ1";
		
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
		table1.setQuotedName(true);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000121CreateTableUnquotedTableSpace() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTable1TS";
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		spec.setTableSpaceData("sodeacdata");
		
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
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000122CreateTableUnquotedTableSpaceAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTable1TS";
		
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
		spec.setTableSpaceData("sodeacdata");
		
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000131CreateTableQuotedTableSpace() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTableQ1TS";
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		
		// create spec
		SchemaSpec spec = new SchemaSpec(databaseID);
		spec.setDbmsSchemaName(testConnection.dbmsSchemaName);
		spec.setTableSpaceData("sodeacdata");
		
		// prepare spec for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
				
		TableSpec table1 = spec.addTable(table1Name);
		table1.setQuotedName(true);
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	public void test000132CreateTableQuotedTableSpaceAgain() throws SQLException, ClassNotFoundException, IOException 
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = DOMAIN;
		String table1Name = "EmptyTableQ1TS";
		
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
		spec.setTableSpaceData("sodeacdata");
		
		TableSpec table1 = spec.addTable(table1Name);
		table1.setQuotedName(true);
		
		// prepare table for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
				
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
					
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
	
	
	
	/*@Test
	public void test0101CreateDBSchema() throws SQLException, ClassNotFoundException, IOException 
	{
		IDatabaseSchemaDriver driver = databaseSchemaProcessor.getDatabaseSchemaDriver(connection);
		
		String databaseID = "DOMAIN";
		String table1Name = "TABLE1";
		String table2Name = "TABLE2";
		
		IMocksControl ctrl = support.createControl();
		IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);
		
		ctrl.checkOrder(true);
		SchemaSpec spec = new SchemaSpec(databaseID);
		
		// prepare for simulation
		Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
		schemaDictionary.put(ObjectType.SCHEMA, spec);
		spec.addUpdateListener(updateListenerMock);
		
		// define table1
		TableSpec table1 = spec.addTable(table1Name);
		
		// prepare for simulation
		Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
		table1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Dictionary.put(ObjectType.TABLE, table1);
		table1.addUpdateListener(updateListenerMock);
			
		ColumnSpec table1Column1 = table1.addColumn(DatabaseCommonElements.ID, ColumnSpec.ColumnType.CHAR, false, 36)
									.setPrimaryKey();
		
		// prepare for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
		table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Column1Dictionary.put(ObjectType.TABLE, table1);
		table1Column1Dictionary.put(ObjectType.COLUMN, table1Column1);
			
		ColumnSpec table1Column2 = table1.addColumn(DatabaseCommonElements.NAME,ColumnSpec.ColumnType.VARCHAR, true, 108);
		
		// prepare simulation
		
		Dictionary<ObjectType, Object> table1Column2Dictionary = new Hashtable<>();
		table1Column2Dictionary.put(ObjectType.SCHEMA, spec);
		table1Column2Dictionary.put(ObjectType.TABLE, table1);
		table1Column2Dictionary.put(ObjectType.COLUMN, table1Column2);
		
		ColumnSpec table1Column3 = table1.addColumn(table2Name + "_" + DatabaseCommonElements.ID,ColumnSpec.ColumnType.CHAR, true, 36)
				.setForeignKey("FK1_" + table1Name, table2Name, DatabaseCommonElements.ID);
		
		// prepare simulation
		
		Dictionary<ObjectType, Object> table1Column3Dictionary = new Hashtable<>();
		table1Column3Dictionary.put(ObjectType.SCHEMA, spec);
		table1Column3Dictionary.put(ObjectType.TABLE, table1);
		table1Column3Dictionary.put(ObjectType.COLUMN, table1Column3);
		
		ColumnSpec table1Column4 = table1.addColumn(DatabaseCommonElements.KEY,ColumnSpec.ColumnType.VARCHAR, true, 108);
		
		// prepare simulation
		
		Dictionary<ObjectType, Object> table1Column4Dictionary = new Hashtable<>();
		table1Column4Dictionary.put(ObjectType.SCHEMA, spec);
		table1Column4Dictionary.put(ObjectType.TABLE, table1);
		table1Column4Dictionary.put(ObjectType.COLUMN, table1Column4);
		
		// index
		
		ColumnIndexSpec indexSpec = table1.addColumnIndex("UNQ1_" + table1Name, new String[] {DatabaseCommonElements.NAME,DatabaseCommonElements.KEY}, true);
		
		Dictionary<ObjectType, Object> table1Index1Dictionary = new Hashtable<>();
		table1Index1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Index1Dictionary.put(ObjectType.TABLE, table1);
		table1Index1Dictionary.put(ObjectType.TABLE_INDEX, indexSpec);
		
		
		// define table2
		TableSpec table2 = spec.addTable(table2Name);
				
		// prepare for simulation
		Dictionary<ObjectType, Object> table2Dictionary = new Hashtable<>();
		table2Dictionary.put(ObjectType.SCHEMA, spec);
		table2Dictionary.put(ObjectType.TABLE, table2);
		table2.addUpdateListener(updateListenerMock);
					
		ColumnSpec table2Column1 = table2.addColumn(DatabaseCommonElements.ID, ColumnSpec.ColumnType.CHAR, false, 36)
										.setPrimaryKey();
				
		// prepare for simulation
				
		Dictionary<ObjectType, Object> table2Column1Dictionary = new Hashtable<>();
		table2Column1Dictionary.put(ObjectType.SCHEMA, spec);
		table2Column1Dictionary.put(ObjectType.TABLE, table2);
		table2Column1Dictionary.put(ObjectType.COLUMN, table2Column1);

		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
			// table creation
		
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary,driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table2Dictionary,driver, null);
			
			// table1 column creation
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column2Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column3Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column4Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column4Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column4Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column4Dictionary, driver, null);
						
			// table2 column creation
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table2Column1Dictionary, driver, null);
			
			// table1 drop keys
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_DROP_KEYS, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_DROP_KEYS, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
									
			// table2 drop keys
							
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_DROP_KEYS, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_DROP_KEYS, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
			
			// convert schema
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
						
			// table1 column properties
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.PRE, connection, databaseID, table1Column2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.POST, connection, databaseID, table1Column2Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.PRE, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.POST, connection, databaseID, table1Column3Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.PRE, connection, databaseID, table1Column4Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.POST, connection, databaseID, table1Column4Dictionary, driver, null);
			
			// table2 column properties
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.PRE, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_PROPERTIES, PhaseType.POST, connection, databaseID, table2Column1Dictionary, driver, null);
			
			
			// table1 create keys/indices
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_KEYS, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_INDICES, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, databaseID, table1Index1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, databaseID, table1Index1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_INDICES, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
			
			// table2 create keys/indices
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CREATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_KEYS, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_INDICES, PhaseType.PRE, connection, databaseID, table2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE_CREATE_INDICES, PhaseType.POST, connection, databaseID, table2Dictionary, driver, null);
			
			// table1 column keys 
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table1Column2Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.POST, connection, databaseID, table1Column2Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, databaseID, table1Column3Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.POST, connection, databaseID, table1Column3Dictionary, driver, null);
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table1Column4Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.POST, connection, databaseID, table1Column4Dictionary, driver, null);
			
			// table2 column keys
			
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.PRE, connection, databaseID, table2Column1Dictionary, driver, null);
			updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN_CREATE_KEYS, PhaseType.POST, connection, databaseID, table2Column1Dictionary, driver, null);
			
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
		
		ctrl.replay();
		
		
		databaseSchemaProcessor.checkSchemaSpec(databaseID,spec, connection);
		
		ctrl.verify();
		
	}*/
	
	

	@Configuration
	public static Option[] config() 
	{
		return Statics.config();
	}
}
