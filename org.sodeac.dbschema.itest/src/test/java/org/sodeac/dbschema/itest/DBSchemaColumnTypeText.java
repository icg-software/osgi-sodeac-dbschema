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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.concurrent.Callable;

import javax.inject.Inject;


@RunWith(PaxExamParameterized.class)
@ExamReactorStrategy(PerSuite.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaColumnTypeText
{
	private EasyMockSupport support = new EasyMockSupport();
	
	public static List<Object[]> connectionList = null;
	public static final Map<String,Boolean> createdSchema = new HashMap<String,Boolean>();
	
	@Inject
	private IDatabaseSchemaProcessor databaseSchemaProcessor;
	
	private String databaseID = "TESTDOMAIN";
	private String table1Name = "TableColChar";
	private String columnCharName = "column_char";
	private String columnVarcharName = "column_varchar";
	private String columnClobName = "column_clob";
	
	@Parameters
    public static List<Object[]> connections()
    {
    	if(connectionList != null)
    	{
    		return connectionList;
    	}
    	return connectionList = Statics.connections(createdSchema);
    }
	
	public DBSchemaColumnTypeText(Callable<TestConnection> connectionFactory)
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
	public void test000300char() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnCharName, IColumnType.ColumnType.CHAR.toString(),true,100);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
		table1Column1Dictionary.put(ObjectType.SCHEMA, spec);
		table1Column1Dictionary.put(ObjectType.TABLE, table1);
		table1Column1Dictionary.put(ObjectType.COLUMN, column1);
		
		// simulate listener
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
		
		// table creation
				
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary,driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
		
		// table1 column creation
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			prepStat = connection.prepareStatement("select " + columnCharName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			assertFalse("rset should contains no more entries", rset.next());
			rset.close();
			prepStat.close();
			
			prepStat = connection.prepareStatement("insert into " +  table1Name + " (" + columnCharName + ") values (?)");
			prepStat.setString(1, "valueforcolumn");
			prepStat.executeUpdate();
			prepStat.close();
			connection.commit();
			
			int count = 0;
			prepStat = connection.prepareStatement("select " + columnCharName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000301charAgain() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnCharName, IColumnType.ColumnType.CHAR.toString(),true,100);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
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
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{

			int count = 0;
			prepStat = connection.prepareStatement("select " + columnCharName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000303charToLong() throws SQLException
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			prepStat = connection.prepareStatement("insert into " +  table1Name + " (" + columnCharName + ") values (?)");
			prepStat.setString(1, "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn");
			prepStat.executeUpdate();
			prepStat.close();
			connection.commit();
			
		}
		catch (SQLException e) 
		{
			return;
		}
		finally 
		{
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
		fail("Expected an SQLException to be thrown");
	}
	
	@Test
	public void test000310varchar() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnVarcharName, IColumnType.ColumnType.VARCHAR.toString(),true,100);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
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
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			
			prepStat = connection.prepareStatement("update " +  table1Name + " set " + columnVarcharName + " = ? ");
			prepStat.setString(1, "value2forcolumn");
			prepStat.executeUpdate();
			prepStat.close();
			connection.commit();
			
			int count = 0;
			prepStat = connection.prepareStatement("select " + columnVarcharName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000311varcharAgain() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnVarcharName, IColumnType.ColumnType.VARCHAR.toString(),true,100);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
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
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{

			int count = 0;
			prepStat = connection.prepareStatement("select " + columnVarcharName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000313varcharToLong() throws SQLException
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			prepStat = connection.prepareStatement("insert into " +  table1Name + " (" + columnVarcharName + ") values (?)");
			prepStat.setString(1, "valueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumnvalueforcolumn");
			prepStat.executeUpdate();
			prepStat.close();
			connection.commit();
			
		}
		catch (SQLException e) 
		{
			return;
		}
		finally 
		{
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
		fail("Expected an SQLException to be thrown");
	}
	
	
	@Test
	public void test000320clob() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnClobName, IColumnType.ColumnType.CLOB.toString(),true);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
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
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1Column1Dictionary, driver, null);
					
		// convert schema
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			
			prepStat = connection.prepareStatement("update " +  table1Name + " set " + columnClobName + " = ? ");
			prepStat.setString(1, "valuelobforcolumn");
			prepStat.executeUpdate();
			prepStat.close();
			connection.commit();
			
			int count = 0;
			prepStat = connection.prepareStatement("select " + columnClobName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000321clobAgain() throws SQLException, ClassNotFoundException, IOException 
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
		
		ColumnSpec column1 = table1.addColumn(columnClobName, IColumnType.ColumnType.CLOB.toString(),true);
		
		// prepare column for simulation
		
		Dictionary<ObjectType, Object> table1Column1Dictionary = new Hashtable<>();
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
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver,  null);
					
		updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
				
		ctrl.replay();
				
		databaseSchemaProcessor.checkSchemaSpec(spec, connection);
		
		ctrl.verify();
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{

			int count = 0;
			prepStat = connection.prepareStatement("select " + columnClobName + " from " +  table1Name);
			rset = prepStat.executeQuery();
			while( rset.next())
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}
	
	@Test
	public void test000323clobLong() throws SQLException
	{
		if(! testConnection.enabled)
		{
			return;
		}
		Connection connection = testConnection.connection;
		
		PreparedStatement prepStat = null;
		ResultSet rset = null;
		try
		{
			connection.setAutoCommit(false);
			
			prepStat = connection.prepareStatement("insert into " +  table1Name + " (" + columnClobName + ") values (?)");
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
			try {rset.close();}catch (Exception e) {}
			try {prepStat.close();}catch (Exception e) {}
		}
	}

	@Configuration
	public static Option[] config() 
	{
		return Statics.config();
	}
}
