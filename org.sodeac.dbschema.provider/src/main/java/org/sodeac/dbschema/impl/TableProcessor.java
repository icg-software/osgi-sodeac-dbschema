/*******************************************************************************
 * Copyright (c) 2018 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.dbschema.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.osgi.service.log.LogService;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.IndexSpec;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

public class TableProcessor
{
	public static TableTracker checkTableDefinition(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, String databaseID, CheckProperties checkProperties)
	{
		TableTracker tableTracker = new TableTracker();
		tableTracker.setTableSpec(table);
		try
		{
			if(table.getUpdateListenerList() != null)
			{
				Dictionary<ObjectType, Object> objects = new Hashtable<>();
				objects.put(ObjectType.SCHEMA, schema);
				objects.put(ObjectType.TABLE, table);
				for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
				{
					try
					{
						updateListener.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, objects, driver, null);
					}
					catch(SQLException e)
					{
						schemaProcessor.logSQLException(e);
					}
					catch (Exception e) 
					{
						schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.Table.Check.Pre ", checkProperties);
					}
					
					if(checkProperties.isInterrupted())
					{
						return tableTracker;
					}
				}
			}
			
			Map<String,Object> tableProperties = new HashMap<String, Object>(); 
			tableTracker.setTableProperties(tableProperties);
			tableTracker.setExits(driver.tableExists(connection, schema, table, tableProperties));
			if(! tableTracker.isExits())
			{
				if((schemaProcessor.logService != null) && schema.getLogUpdates())
				{
					schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=createtable)(database=" + databaseID + ")(object=" + table.getName() + ")} create table " + table.getName());
				}
					
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, objects, driver, null);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.Table.Insert.Pre ", checkProperties);
						}
						
						if(checkProperties.isInterrupted())
						{
							return tableTracker;
						}
					}
				}
				
				Exception exc= null;
				
				try
				{
					driver.createTable(connection, schema, table, tableProperties);
					
					tableTracker.setCreated(true);
					tableTracker.setExits(true);
				}
				catch(SQLException e)
				{
					exc = e;
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					exc = e;
					schemaProcessor.logError(e, schema,  "Table " + table.getName() + " can not create ", checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return tableTracker;
				}
					
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, objects, driver, exc);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.Table.Insert.Post ", checkProperties);
						}
						
						if(checkProperties.isInterrupted())
						{
							return tableTracker;
						}
					}
				}
			}
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
		}
		catch (Exception e) 
		{
			schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error", checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return tableTracker;
		}
		
		if(table.getUpdateListenerList() != null)
		{
			Dictionary<ObjectType, Object> objects = new Hashtable<>();
			objects.put(ObjectType.SCHEMA, schema);
			objects.put(ObjectType.TABLE, table);
			for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
			{
				try
				{
					updateListener.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, objects, driver, null);
				}
				catch(SQLException e)
				{
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.Table.Check.Post",checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return tableTracker;
				}
			}
		}
			
		try
		{
			SQLWarning warning = connection.getWarnings();
			if(warning != null)
			{
				schemaProcessor.logSQLException(warning);
			}
			connection.clearWarnings();
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
			try
			{
				connection.clearWarnings();
			}
			catch(Exception e2){}
		}
		
		return tableTracker;
	}
	
	public static void createTableKeys(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, TableTracker tableTracker, String databaseID, CheckProperties checkProperties)
	{
		if(! tableTracker.isExits())
		{
			return;
		}
		
		try
		{
			boolean pkExists = driver.primaryKeyExists(connection, schema, table, tableTracker.getTableProperties());
			if(! pkExists)
			{
				if((schemaProcessor.logService != null) && schema.getLogUpdates())
				{
					schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO,  "{(type=updatedbmodel)(action=createprimarykey)(database=" + databaseID + ")(object=" + table.getName() + ")} create primarykey " + table.getName() );
				}
					
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, objects, driver,  null);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.PrimaryKey.Pre ",  checkProperties);
						}
						
						if(checkProperties.isInterrupted())
						{
							return;
						}
					}
				}
				
				Exception exc= null;
				
				try
				{
					driver.setPrimaryKey(connection, schema, table, tableTracker.getTableProperties());
				}
				catch(SQLException e)
				{
					exc = e;
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					exc = e;
					schemaProcessor.logError(e, schema,  "Primary Key for Table " + table.getName() + " can not create ",  checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return;
				}
						
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, objects, driver,  exc);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.PrimaryKey.Post ",  checkProperties);
						}
						
						if(checkProperties.isInterrupted())
						{
							return;
						}
					}
				}
			}
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
		}
		catch (Exception e) 
		{
			schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error handle primary key ",checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return;
		}
		
		try
		{
			SQLWarning warning = connection.getWarnings();
			if(warning != null)
			{
				schemaProcessor.logSQLException(warning);
			}
			connection.clearWarnings();
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
			try
			{
				connection.clearWarnings();
			}
			catch(Exception e2){}
		}
	}
	
	public static void createTableIndices(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, TableTracker tableTracker, String databaseID, CheckProperties checkProperties)
	{
		if(! tableTracker.isExits())
		{
			return;
		}
		
		try
		{
			if(table.getColumnIndexList() != null)
			{
				for(IndexSpec indexSpec : table.getColumnIndexList())
				{
					try
					{
						Map<String,Object> columnIndexProperties = new HashMap<String,Object>();
						boolean indexExists = driver.isValidIndex(connection, schema, table, indexSpec, columnIndexProperties);
						
						if(! indexExists)
						{
							
							if((schemaProcessor.logService != null) && schema.getLogUpdates())
							{
								schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO,  "{(type=updatedbmodel)(action=createindex)(database=" + databaseID + ")(object=" + table.getName() + ")} create index " + indexSpec.getIndexName() );
							}
							
							if(table.getUpdateListenerList() != null)
							{
								Dictionary<ObjectType, Object> objects = new Hashtable<>();
								objects.put(ObjectType.SCHEMA, schema);
								objects.put(ObjectType.TABLE, table);
								objects.put(ObjectType.TABLE_INDEX, indexSpec);
								for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
								{
									try
									{
										updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.PRE, connection, databaseID, objects, driver,  null);
									}
									catch(SQLException e)
									{
										schemaProcessor.logSQLException(e);
									}
									catch (Exception e) 
									{
										schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Error on UpdateListener.Index.Pre", checkProperties);
									}
									
									if(checkProperties.isInterrupted())
									{
										return;
									}
								}
							}
							
							Exception exc = null;
							try
							{
								driver.setValidIndex(connection, schema, table, indexSpec, columnIndexProperties);
							}
							catch(SQLException e)
							{
								exc = e;
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								exc = e;
								schemaProcessor.logError(e, schema,  "Index " + indexSpec.getIndexName() + " can not create ", checkProperties);
							}
								
							if(checkProperties.isInterrupted())
							{
								return;
							}
							
							if(table.getUpdateListenerList() != null)
							{
								Dictionary<ObjectType, Object> objects = new Hashtable<>();
								objects.put(ObjectType.SCHEMA, schema);
								objects.put(ObjectType.TABLE, table);
								objects.put(ObjectType.TABLE_INDEX, indexSpec);
								for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
								{
									try
									{
										updateListener.onAction(ActionType.UPDATE, ObjectType.TABLE_INDEX, PhaseType.POST, connection, databaseID, objects, driver,  exc);
									}
									catch(SQLException e)
									{
										schemaProcessor.logSQLException(e);
									}
									catch (Exception e) 
									{
										schemaProcessor.logError(e, schema, "Table " + table.getName() + " Error on UpdateListener.Index.Post", checkProperties);
									}
									
									if(checkProperties.isInterrupted())
									{
										return;
									}
								}
							}
						}
					}
					catch(SQLException e)
					{
						schemaProcessor.logSQLException(e);
					}
					catch (Exception e) 
					{
						schemaProcessor.logError(e, schema, "error: " + indexSpec.getIndexName(), checkProperties);
					}
				}
			
			}
		}
		catch (Exception e) 
		{
			schemaProcessor.logError(e, schema,e.getMessage(), checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return;
		}
			
		try
		{
			SQLWarning warning = connection.getWarnings();
			if(warning != null)
			{
				schemaProcessor.logSQLException(warning);
			}
			connection.clearWarnings();
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
			try
			{
				connection.clearWarnings();
			}
			catch(Exception e2){}
		}
	}
}
