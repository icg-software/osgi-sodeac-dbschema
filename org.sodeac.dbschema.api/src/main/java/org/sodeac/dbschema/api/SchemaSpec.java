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
package org.sodeac.dbschema.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Specification of schema.
 * 
 * @author Sebastian Palarus
 *
 */
public class SchemaSpec 
{
	/**
	 * Creates new  schema-specification for {@code domain} with dbms schema name of domain 
	 *  
	 * @param domain name of domain and dbms schema name
	 */
	public SchemaSpec(String domain)
	{
		super();
		this.domain = domain;
		this.dbmsSchemaName = this.domain;
	}
	
	/**
	 * Creates new schema-specification for {@code domain} with {@code dbmsSchemaName}
	 * 
	 * @param domain name of domain
	 * @param dbmsSchemaName name of schema
	 */
	public SchemaSpec(String domain, String dbmsSchemaName)
	{
		super();
		this.domain = domain;
		this.dbmsSchemaName = dbmsSchemaName;
	}
	
	private List<TableSpec> listTableSpec = new ArrayList<TableSpec>();
	private String domain = null;
	private String dbmsSchemaName = null;
	private String tableSpaceData = null;
	private String tableSpaceIndex = null;
	private List<IDatabaseSchemaUpdateListener> updateListenerList = null;
	private boolean skipChecks = false;
	private boolean logUpdates = true;

	/**
	 * getter for list of  table specifications
	 * 
	 * @return list of table specs
	 */
	public List<TableSpec> getListTableSpec() 
	{
		return  listTableSpec;
	}
	
	/**
	 * find and return table with given {@code name}
	 * 
	 * @param name name of table to return
	 * 
	 * @return table specification
	 */
	public TableSpec getTable(String name)
	{
		for(TableSpec table : this.listTableSpec)
		{
			if(table.getName().equals(name))
			{
				return table;
			}
		}
		return null;
	}
	
	/**
	 * getter for domain
	 * 
	 * @return domain
	 */
	public String getDomain() 
	{
		return domain;
	}
	
	/**
	 * getter for dbms schema name
	 * 
	 * @return schema name
	 */
	public String getDbmsSchemaName()
	{
		return dbmsSchemaName;
	}
	
	/**
	 * setter for dbms schema name
	 * 
	 * @param dbmsSchemaName name of schema in database
	 * @return schema spec
	 */
	public SchemaSpec setDbmsSchemaName(String dbmsSchemaName)
	{
		this.dbmsSchemaName = dbmsSchemaName;
		return this;
	}
	
	/**
	 * create and append table specification
	 * 
	 * @param name tablename
	 * 
	 * @return created tablespec
	 */
	public TableSpec addTable(String name)
	{
		TableSpec tableSpec = new TableSpec(name, this);
		this.listTableSpec.add(tableSpec);
		return tableSpec;
	}
	
	/**
	 * getter for updatelistener list
	 * 
	 * @return updatelistener list
	 */
	public List<IDatabaseSchemaUpdateListener> getUpdateListenerList()
	{
		return updateListenerList;
	}
	
	/**
	 * Append updateListener for schema update process, if not exists
	 * 
	 * @param updateListener for updateListener for schema update process
	 */
	public void addUpdateListener(IDatabaseSchemaUpdateListener updateListener)
	{
		if(this.updateListenerList == null)
		{
			this.updateListenerList = new ArrayList<IDatabaseSchemaUpdateListener>();
		}
		for(IDatabaseSchemaUpdateListener exists : this.updateListenerList)
		{
			if(updateListener == exists)
			{
				return;
			}
		}
		this.updateListenerList.add(updateListener);
	}
	
	/**
	 * Getter for used default data tablespace.
	 * 
	 * @return default data tablespace
	 */
	public String getTableSpaceData()
	{
		return tableSpaceData;
	}
	
	/**
	 * Setter for default data tablespace (dbms must support tablespaces). This tablespace is used for tablecreation and can overwrite in tablespec.
	 * 
	 * @param tableSpaceData tablespace name of default data tablespace
	 * @return schemaSpec
	 */
	public SchemaSpec setTableSpaceData(String tableSpaceData)
	{
		this.tableSpaceData = tableSpaceData;
		return this;
	}
	
	/**
	 * Getter for used default index tablespace.
	 * 
	 * @return defualt index tablespace
	 */
	public String getTableSpaceIndex()
	{
		return tableSpaceIndex;
	}
	
	/**
	 * Setter for default index tablespace (dbms must support tablespaces). This tablespace is used for indexcreation (pk, indices) and can overwrite in tablespec or indexspec
	 * 
	 * @param tableSpaceIndex tablespace name of default index tablespace
	 * @return schemaspec
	 */
	public SchemaSpec setTableSpaceIndex(String tableSpaceIndex)
	{
		this.tableSpaceIndex = tableSpaceIndex;
		return this;
	}
	
	/**
	 * Getter for skip check optionflag (pk,constraints, key, indices, nullable-options)
	 * 
	 * @return option to skip checks
	 */
	public boolean getSkipChecks()
	{
		return skipChecks;
	}
	
	/**
	 * Setter for skip check-optionflag. If set true, the schemaprocessor not creates or updates primary keys, constraints, keys, indices and nullable-option.
	 * 
	 * @param skipChecks check-optionflag
	 * @return schema spec
	 */
	public SchemaSpec setSkipChecks(boolean skipChecks)
	{
		this.skipChecks = skipChecks;
		return this;
	}
	
	/**
	 * Apply a schema template, to create standard objects
	 * 
	 * @param schemaTemplateClass class of template
	 * @return schema spec
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public SchemaSpec applyTemplate(Class<?> schemaTemplateClass) throws InstantiationException, IllegalAccessException
	{
		ISchemaTemplate instanceOfTemplate = (ISchemaTemplate)schemaTemplateClass.newInstance();
		instanceOfTemplate.schemaTemplateApply(this);
		return this;
	}
	
	
	/**
	 * getter for flag to log schema updates
	 * 
	 * @return true, flag to log schema updates is enabled, otherwise false
	 */
	public boolean getLogUpdates()
	{
		return logUpdates;
	}

	/**
	 * set flag to log schema updates
	 * 
	 * @param logUpdates flag to log schema updates 
	 */
	public void setLogUpdates(boolean logUpdates)
	{
		this.logUpdates = logUpdates;
	}

	@Override
	public String toString()
	{
		return "SchemaSpec " + domain + " / " + dbmsSchemaName;
	}
	
}
