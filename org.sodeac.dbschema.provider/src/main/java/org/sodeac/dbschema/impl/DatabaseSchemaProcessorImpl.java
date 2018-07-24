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
package org.sodeac.dbschema.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.log.LogService;
import org.sodeac.dbschema.api.IDatabaseSchemaProcessor;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.SchemaUnusableException;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.api.TerminateException;

@Component(name="DatabaseSchemaProcessor", service=IDatabaseSchemaProcessor.class)
public class DatabaseSchemaProcessorImpl implements IDatabaseSchemaProcessor 
{
	protected volatile ComponentContext context = null;
	
	@Reference(cardinality=ReferenceCardinality.OPTIONAL,policy=ReferencePolicy.DYNAMIC)
	protected volatile LogService logService = null;
	
	private DriverManager<IDatabaseSchemaDriver> schemaDriverList = new DriverManager<IDatabaseSchemaDriver>();
	private DriverManager<IColumnType> columnDriverList = new DriverManager<IColumnType>();
	
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,policy=ReferencePolicy.DYNAMIC)
	public void bindSchemaDriver(IDatabaseSchemaDriver type, ServiceReference<IDatabaseSchemaDriver> serviceReference)
	{
		this.schemaDriverList.add(type,serviceReference);
	}
	public void unbindSchemaDriver(IDatabaseSchemaDriver type, ServiceReference<IDatabaseSchemaDriver> serviceReference)
	{
		this.schemaDriverList.remove(type,serviceReference);
	}
	
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,policy=ReferencePolicy.DYNAMIC)
	public void bindColumnType(IColumnType type, ServiceReference<IColumnType> serviceReference)
	{
		this.columnDriverList.add(type,serviceReference);
	}
	public void unbindColumnType(IColumnType type, ServiceReference<IColumnType> serviceReference)
	{
		this.columnDriverList.remove(type,serviceReference);
	}
	
	@Activate
	private void activate(ComponentContext context, Map<String, ?> properties)
	{
		this.context = context;
	}
	
	@Deactivate
	private void deactivate(ComponentContext context)
	{
		this.context = null;
	}
	
	@Override
	public IDatabaseSchemaDriver getDatabaseSchemaDriver(Connection connection) throws SQLException
	{
		int currentLevel = -1;
		IDatabaseSchemaDriver currentDriver = null;
		for(IDatabaseSchemaDriver driver : this.schemaDriverList.getDriverList())
		{
			int level = driver.handle(connection);
			if(level > -1)
			{
				if(level > currentLevel)
				{
					currentLevel = level;
					currentDriver = driver;
				}
			}
		}
		
		if(currentDriver != null)
		{
			currentDriver.setColumnDriverList(this.columnDriverList.getDriverList());
		}
		
		return currentDriver;
	}
		
	@Override
	public boolean checkSchemaSpec(SchemaSpec schemaSpec, Connection connection) throws SQLException
	{
		
		if(schemaSpec == null)
		{
			return false;
		}
		if(schemaSpec.getDomain() == null)
		{
			return false;
		}
		if(schemaSpec.getDomain().isEmpty())
		{
			return false;
		}
		
		CheckProperties checkProperties = new CheckProperties();
		String domain = schemaSpec.getDomain();
		
		
		IDatabaseSchemaDriver driver = this.getDatabaseSchemaDriver(connection);
		
		if(driver == null)
		{
			return false;
		}
		
		
		if(schemaSpec.getUpdateListenerList() != null)
		{
			
			Dictionary<ObjectType, Object> objects = new Hashtable<>();
			objects.put(ObjectType.SCHEMA, schemaSpec);
			for(IDatabaseSchemaUpdateListener updateListener : schemaSpec.getUpdateListenerList())
			{
				try
				{
					updateListener.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, domain, objects,driver, null);
				}
				catch (Exception e) 
				{
					this.logError(e, schemaSpec, "Error on UpdateListener.Schema.Check.Pre " + schemaSpec.getDomain(),checkProperties);
				}
			}
		}
		
		List<TableTracker> tableTrackerList = new ArrayList<TableTracker>();
		
		// create tables
		
		if(schemaSpec.getListTableSpec() != null)
		{
			for(TableSpec tableSpec : schemaSpec.getListTableSpec())
			{
				try
				{
					tableTrackerList.add(TableProcessor.checkTableDefinition(this, connection, driver, schemaSpec, tableSpec, domain, checkProperties));
				}
				catch (Exception e) 
				{
					this.logError(e, schemaSpec, "Error on checkSchema " + schemaSpec.getDomain(), checkProperties);
				}
				if(checkProperties.isInterrupted())
				{
					return checkProperties.getUnusableExceptionList().isEmpty();
				}
			}
		}
		
		// create columns
		
		for(TableTracker tableTracker : tableTrackerList)
		{
			if(tableTracker.isExits())
			{
				if(tableTracker.getTableSpec().getColumnList() != null)
				{
					for(ColumnSpec columnSpec : tableTracker.getTableSpec().getColumnList())
					{
						tableTracker.getColumnTrackerList().add(ColumnProcessor.checkColumnDefinition
						(
							this, connection, driver, schemaSpec, tableTracker.getTableSpec(), columnSpec, domain, tableTracker.getTableProperties(), checkProperties
						));
						
						if(checkProperties.isInterrupted())
						{
							return checkProperties.getUnusableExceptionList().isEmpty();
						}
					}
				}
			}
		}
		
		// schema convert phase
		
		try
		{
			if(schemaSpec.getUpdateListenerList() != null)
			{
				Dictionary<ObjectType, Object> objects = new Hashtable<>();
				objects.put(ObjectType.SCHEMA, schemaSpec);
				for(IDatabaseSchemaUpdateListener updateListener : schemaSpec.getUpdateListenerList())
				{
					try
					{
						updateListener.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, domain, objects, driver,  null);
					}
					catch(SQLException e)
					{
						logSQLException(e);
					}
					catch (Exception e) 
					{
						this.logError(e, schemaSpec, "Convert Schema " + schemaSpec.getDomain() + " Error on UpdateListener.Schema.Check.Pre ", checkProperties);
					}
					
					if(checkProperties.isInterrupted())
					{
						return checkProperties.getUnusableExceptionList().isEmpty();
					}
				}
				
				for(IDatabaseSchemaUpdateListener updateListener : schemaSpec.getUpdateListenerList())
				{
					try
					{
						updateListener.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, domain, objects, driver,  null);
					}
					catch(SQLException e)
					{
						logSQLException(e);
					}
					catch (Exception e) 
					{
						this.logError(e, schemaSpec, "Convert Schema " + schemaSpec.getDomain() + " Error on UpdateListener.Schema.Update.Pre ", checkProperties);
					}
					
					try
					{
						updateListener.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, domain, objects, driver,  null);
					}
					catch(SQLException e)
					{
						logSQLException(e);
					}
					catch (Exception e) 
					{
						this.logError(e, schemaSpec, "Convert Schema " + schemaSpec.getDomain() + " Error on UpdateListener.Schema.Update.Post ", checkProperties);
					}
					
					if(checkProperties.isInterrupted())
					{
						return checkProperties.getUnusableExceptionList().isEmpty();
					}
				}
				
				for(IDatabaseSchemaUpdateListener updateListener : schemaSpec.getUpdateListenerList())
				{
					try
					{
						updateListener.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, domain, objects, driver,  null);
					}
					catch(SQLException e)
					{
						logSQLException(e);
					}
					catch (Exception e) 
					{
						this.logError(e, schemaSpec, "Convert Schema " + schemaSpec.getDomain() + " Error on UpdateListener.Schema.Check.Post ", checkProperties);
					}
					
					if(checkProperties.isInterrupted())
					{
						return checkProperties.getUnusableExceptionList().isEmpty();
					}
				}
			}
		}
		catch(Exception e)
		{
			this.logError(e, schemaSpec, "Error on schema ConvertPhase " + schemaSpec.getDomain(), checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return checkProperties.getUnusableExceptionList().isEmpty();
		}
			
		try
		{
			SQLWarning warning = connection.getWarnings();
			if(warning != null)
			{
				logSQLException(warning);
			}
			connection.clearWarnings();
		}
		catch(SQLException e)
		{
			logSQLException(e);
			try
			{
				connection.clearWarnings();
			}
			catch(Exception e2){}
		}
		
		// column properties
		
		for(TableTracker tableTracker : tableTrackerList)
		{
			if(tableTracker.isExits())
			{
				if(tableTracker.getTableSpec().getColumnList() != null)
				{
					for(ColumnTracker columnTracker : tableTracker.getColumnTrackerList())
					{
						if(columnTracker.isExits())
						{
							boolean backupNullable = columnTracker.getColumnSpec().getNullable();
							if(schemaSpec.getSkipChecks())
							{
								columnTracker.getColumnSpec().setNullable(true);
							}
							try
							{
								ColumnProcessor.checkColumnProperties
								(
									this, connection, driver, schemaSpec, tableTracker.getTableSpec(), columnTracker.getColumnSpec(), columnTracker, domain, columnTracker.getColumnProperties(),checkProperties
								);
							}
							finally 
							{
								if(schemaSpec.getSkipChecks())
								{
									columnTracker.getColumnSpec().setNullable(backupNullable);
								}
							}
						}
						if(checkProperties.isInterrupted())
						{
							return checkProperties.getUnusableExceptionList().isEmpty();
						}
					}
				}
			}
		}
		
		if(! schemaSpec.getSkipChecks())
		{
			for(TableTracker tableTracker : tableTrackerList)
			{
				if(tableTracker.isExits())
				{
					TableProcessor.createTableKeys(this, connection, driver, schemaSpec, tableTracker.getTableSpec(), tableTracker, domain, checkProperties);
					
					if(checkProperties.isInterrupted())
					{
						return checkProperties.getUnusableExceptionList().isEmpty();
					}
					
					TableProcessor.createTableIndices(this, connection, driver, schemaSpec, tableTracker.getTableSpec(), tableTracker, domain, checkProperties);
					
					if(checkProperties.isInterrupted())
					{
						return checkProperties.getUnusableExceptionList().isEmpty();
					}
				}
			}
			
			for(TableTracker tableTracker : tableTrackerList)
			{
				if(tableTracker.isExits())
				{
					if(tableTracker.getTableSpec().getColumnList() != null)
					{
						for(ColumnTracker columnTracker : tableTracker.getColumnTrackerList())
						{
							if(columnTracker.isExits())
							{
								ColumnProcessor.createColumnKeys
								(
									this, connection, driver, schemaSpec, tableTracker.getTableSpec(), columnTracker.getColumnSpec(), columnTracker, domain, columnTracker.getColumnProperties(), checkProperties
								);
								
								if(checkProperties.isInterrupted())
								{
									return checkProperties.getUnusableExceptionList().isEmpty();
								}
							}
						}
					}
				}
			}
		}
		
		try
		{
			driver.dropDummyColumns(connection, schemaSpec);
		}
		catch(SQLException e)
		{
			logSQLException(e);
		}
		catch (Exception e) 
		{
			this.logError(e, schemaSpec, "Error on drop dummy columns " + schemaSpec.getDomain(), checkProperties);
		}
		
		if(checkProperties.isInterrupted())
		{
			return checkProperties.getUnusableExceptionList().isEmpty();
		}
		
		if(schemaSpec.getUpdateListenerList() != null)
		{
			
			Dictionary<ObjectType, Object> objects = new Hashtable<>();
			objects.put(ObjectType.SCHEMA, schemaSpec);
			for(IDatabaseSchemaUpdateListener updateListener : schemaSpec.getUpdateListenerList())
			{
				try
				{
					updateListener.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, domain, objects, driver, null);
				}
				catch(SQLException e)
				{
					logSQLException(e);
				}
				catch (Exception e) 
				{
					this.logError(e, schemaSpec, "Error on UpdateListener.Schema.Check.Post " + schemaSpec.getDomain(), checkProperties);
				}
				
				if(checkProperties.isInterrupted())
				{
					return checkProperties.getUnusableExceptionList().isEmpty();
				}
			}
		}
		
		return checkProperties.getUnusableExceptionList().isEmpty();
	}
	
	protected void logSQLException(SQLException e)
	{
		if(this.logService == null)
		{
			e.printStackTrace();
			return;
		}
		
		this.logService.log(this.context == null ? null : this.context.getServiceReference(), LogService.LOG_ERROR, "{(type=sqlerror)(sqlstate=" + e.getSQLState() + ")(errorcode=" + e.getErrorCode() + ")} " + e.getMessage(),e);
	
		SQLException nextException = e.getNextException();
		if(nextException != null)
		{
			logSQLException(nextException);
		}
		
		if(e instanceof SQLWarning)
		{
			SQLWarning nextWarning = ((SQLWarning)e).getNextWarning();
			if(nextWarning == null)
			{
				return;
			}
			if(nextException == null)
			{
				logSQLException(nextWarning);
				return;
			}
			if(nextException != nextWarning)
			{
				logSQLException(nextWarning);
			}
		}
		
	}
	
	protected void logError(Throwable throwable, SchemaSpec schema, String msg, CheckProperties checkProperties)
	{
		if( throwable instanceof SchemaUnusableException) 
		{
			checkProperties.getUnusableExceptionList().add((SchemaUnusableException)throwable);
			if(throwable.getCause() instanceof TerminateException )
			{
				checkProperties.setInterrupted(true);
			}
		}
		if( throwable instanceof TerminateException) 
		{
			checkProperties.setInterrupted(true);
		}
		
		if(this.logService != null)
		{
			this.logService.log(this.context == null ? null : this.context.getServiceReference(), LogService.LOG_ERROR, msg,throwable);
		}
		else
		{
			System.err.println("" + msg);
			if(throwable != null)
			{
				throwable.printStackTrace();
			}
		}
	}
}
