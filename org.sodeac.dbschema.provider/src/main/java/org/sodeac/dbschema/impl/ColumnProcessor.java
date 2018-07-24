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
import java.util.Map.Entry;

import org.osgi.service.log.LogService;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

public class ColumnProcessor
{
	public static ColumnTracker checkColumnDefinition(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, ColumnSpec column, String databaseID, Map<String,Object> tableProperties, CheckProperties checkProperties)
	{
		ColumnTracker columnTracker = new ColumnTracker();
		columnTracker.setColumnSpec(column);
		try
		{
			if(table.getUpdateListenerList() != null)
			{
				Dictionary<ObjectType, Object> objects = new Hashtable<>();
				objects.put(ObjectType.SCHEMA, schema);
				objects.put(ObjectType.TABLE, table);
				objects.put(ObjectType.COLUMN, column);
				for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
				{
					try
					{
						updateListener.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, objects, driver,  null);
					}
					catch(SQLException e)
					{
						schemaProcessor.logSQLException(e);
					}
					catch (Exception e) 
					{
						schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Column.Check.Pre ", checkProperties);
					}
					
					if(checkProperties.isInterrupted())
					{
						return columnTracker;
					}
				}
			}
			
			Map<String,Object> columnProperties = new HashMap<String, Object>(); 
			if(tableProperties != null)
			{
				for(Entry<String, Object> entry : tableProperties.entrySet())
				{
					columnProperties.put(entry.getKey(), entry.getValue());
				}
			}
			columnTracker.setColumnProperties(columnProperties);
			columnTracker.setExits(driver.columnExists(connection, schema, table,column, columnProperties));
				
			if(! columnTracker.isExits())
			{
				if((schemaProcessor.logService != null) && schema.getLogUpdates())
				{
					schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=createcolumn)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} create column " + table.getName().toUpperCase() + "." + column.getName());
				}
					
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					objects.put(ObjectType.COLUMN, column);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, objects, driver,  null);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Column.Insert.Pre ", checkProperties);
						}
						
						if(checkProperties.isInterrupted())
						{
							return columnTracker;
						}
					}
				}
				
				Exception exc= null;
				
				try
				{
					driver.createColumn(connection, schema, table, column, columnProperties);
					
					columnTracker.setExits(true);
					columnTracker.setCreated(true);
				}
				catch(SQLException e)
				{
					exc = e;
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					exc = e;
					schemaProcessor.logError(e, schema,  "Column " + table.getName() + "." + column.getName() + " can not create ",  checkProperties);
				}
					
				if(checkProperties.isInterrupted())
				{
					return columnTracker;
				}
				
				if(columnTracker.isCreated())
				{
					if(table.getUpdateListenerList() != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, objects, driver,  exc);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Column.Insert.Post ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return columnTracker;
							}
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
			schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error ", checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return columnTracker;
		}
		
		if(table.getUpdateListenerList() != null)
		{
			Dictionary<ObjectType, Object> objects = new Hashtable<>();
			objects.put(ObjectType.SCHEMA, schema);
			objects.put(ObjectType.TABLE, table);
			objects.put(ObjectType.COLUMN, column);
			for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
			{
				try
				{
					updateListener.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, objects, driver,  null);
				}
				catch(SQLException e)
				{
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Column.Check.Post",  checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return columnTracker;
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
		
		return columnTracker;
	}
	
	public static void checkColumnProperties(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, ColumnSpec column, ColumnTracker columnTracker, String databaseID, Map<String,Object> columnProperties, CheckProperties checkProperties)
	{
		if(! columnTracker.isExits())
		{
			return;
		}
		try
		{
			boolean columnPropertiesValid = driver.isValidColumnProperties(connection, schema, table, column, columnProperties);
			
			if(! columnPropertiesValid)
			{
				if(columnProperties.get("INVALID_NULLABLE") != null)
				{
					if((schemaProcessor.logService != null) && schema.getLogUpdates())
					{
						schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=setnullable)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} set nullable " + table.getName() + "." + column.getName() + " " + column.getNullable());
					}
					
					if(table.getUpdateListenerList() != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, objects, driver,  null);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Nullable.Pre ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
				}
				
				if(columnProperties.get("INVALID_SIZE") != null)
				{
					if((schemaProcessor.logService != null) && schema.getLogUpdates())
					{
						schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=setcolsize)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} set colsize " + table.getName() + "." + column.getName() + " " + column.getSize());
					}
					
					if(table.getUpdateListenerList() != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_SIZE, PhaseType.PRE, connection, databaseID, objects, driver,  null);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Size.Pre ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
				}
					
					
				if(columnProperties.get("INVALID_TYPE") != null)
				{
					if((schemaProcessor.logService != null) && schema.getLogUpdates())
					{
						schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=setcoltype)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} set type " + table.getName() + "." + column.getName() + " " + column.getColumntype());
					}
					if(table.getUpdateListenerList() != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_TYPE, PhaseType.PRE, connection, databaseID, objects, driver,  null);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Type.Pre ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
				}
					
				if(columnProperties.get("INVALID_DEFAULT") != null)
				{
					if((schemaProcessor.logService != null) && schema.getLogUpdates())
					{
						schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=setcoldefaultvalue)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} set nullable " + table.getName() + "." + column.getName() + " " + column.getDefaultValue());
					}
					
					if(table.getUpdateListenerList() != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.PRE, connection, databaseID, objects, driver,  null);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.defaultvalue.Pre ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
				}
					
					
				Exception exc = null;
				
				try
				{
					driver.setValidColumnProperties(connection, schema, table, column, columnProperties);
				}
				catch(SQLException e)
				{
					exc = e;
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					exc = e;
					schemaProcessor.logError(e, schema,  "Column properties for " + table.getName() + "." + column.getName() + " can not update", checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return;
				}
				
				if(schema.getUpdateListenerList() != null)
				{
					if(columnProperties.get("INVALID_NULLABLE") != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, objects, driver,  exc);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Nullable.Post ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
						
					if(columnProperties.get("INVALID_SIZE") != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_SIZE, PhaseType.POST, connection, databaseID, objects, driver,  exc);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Size.Post ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
						
							
					if(columnProperties.get("INVALID_TYPE") != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_TYPE, PhaseType.POST, connection, databaseID, objects, driver,  exc);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.Type.Post ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
							
					if(columnProperties.get("INVALID_DEFAULT") != null)
					{
						Dictionary<ObjectType, Object> objects = new Hashtable<>();
						objects.put(ObjectType.SCHEMA, schema);
						objects.put(ObjectType.TABLE, table);
						objects.put(ObjectType.COLUMN, column);
						for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
						{
							try
							{
								updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_DEFAULT_VALUE, PhaseType.POST, connection, databaseID, objects, driver,  exc);
							}
							catch(SQLException e)
							{
								schemaProcessor.logSQLException(e);
							}
							catch (Exception e) 
							{
								schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.defaultvalu.Post ", checkProperties);
							}
							
							if(checkProperties.isInterrupted())
							{
								return;
							}
						}
					}
				}
				
			}
		}
		catch(SQLException e)
		{
			schemaProcessor.logSQLException(e);
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
	
	public static void createColumnKeys(DatabaseSchemaProcessorImpl schemaProcessor, Connection connection, IDatabaseSchemaDriver driver, SchemaSpec schema, TableSpec table, ColumnSpec column, ColumnTracker columnTracker, String databaseID, Map<String,Object> columnProperties, CheckProperties checkProperties)
	{
		if(! columnTracker.isExits())
		{
			return;
		}
		try
		{
			boolean foreinKeyValid = driver.isValidForeignKey(connection, schema, table, column, columnTracker.getColumnProperties());
			if(! foreinKeyValid)
			{
				if((schemaProcessor.logService != null) && schema.getLogUpdates())
				{
					schemaProcessor.logService.log(schemaProcessor.context == null ? null : schemaProcessor.context.getServiceReference(), LogService.LOG_INFO, "{(type=updatedbmodel)(action=createforeignkey)(database=" + databaseID + ")(object=" + table.getName() + "." + column.getName() + ")} create foreignkey " + table.getName() + "." + column.getName());
				}
				
				if(table.getUpdateListenerList() != null)
				{
					Dictionary<ObjectType, Object> objects = new Hashtable<>();
					objects.put(ObjectType.SCHEMA, schema);
					objects.put(ObjectType.TABLE, table);
					objects.put(ObjectType.COLUMN, column);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, databaseID, objects, driver,  null);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.FK.Pre ", checkProperties);
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
					driver.setValidForeignKey(connection, schema, table, column, columnProperties);
				}
				catch(SQLException e)
				{
					exc = e;
					schemaProcessor.logSQLException(e);
				}
				catch (Exception e) 
				{
					exc = e;
					schemaProcessor.logError(e, schema,  "Column foreign key for " + table.getName() + "." + column.getName() + " can not update", checkProperties);
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
					objects.put(ObjectType.COLUMN, column);
					for(IDatabaseSchemaUpdateListener updateListener : schema.getUpdateListenerList())
					{
						try
						{
							updateListener.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, databaseID, objects, driver,  exc);
						}
						catch(SQLException e)
						{
							schemaProcessor.logSQLException(e);
						}
						catch (Exception e) 
						{
							schemaProcessor.logError(e, schema,  "Table " + table.getName() + " Col " + column.getName() + " Error on UpdateListener.FK.Post", checkProperties);
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
