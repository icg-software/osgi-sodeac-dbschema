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
 * Specification of table
 * 
 * @author Sebastian Palarus
 *
 */
public class TableSpec 
{
	private String name = null;
	private String dbmsSchemaName = null;
	private List<ColumnSpec> columnList = new ArrayList<ColumnSpec>();
	private List<IndexSpec> columnIndexList = new ArrayList<IndexSpec>();
	private List<IDatabaseSchemaUpdateListener> updateListenerList = null;
	private SchemaSpec schema = null;
	private Boolean quotedName = null;
	private String tableSpace = null;
	
	/**
	 * 
	 * 
	 * Creates new table-specification for {@code schema} with {@code name} 
	 * 
	 * @param name table name
	 * @param schema schema specification
	 */
	public TableSpec(String name, SchemaSpec schema)
	{
		super();
		this.name = name;
		this.schema = schema;
	}
	
	/**
	 * create and append a column specification
	 * 
	 * @param name column-name
	 * @param columntype string represent of {@link IColumnType}, for example IColumnType.ColumnType.VARCHAR.toString() 
	 * 
	 * @return created column specification
	 */
	public ColumnSpec addColumn(String name,String columntype)
	{
		ColumnSpec columnSpec = new ColumnSpec(this,name,columntype);
		this.columnList.add(columnSpec);
		return columnSpec;
	}
	
	/**
	 *
	 * create and append a column specification
	 * 
	 * @param name column-name
	 * @param columntype string represent of {@link IColumnType}, for example {@code IColumnType.ColumnType.VARCHAR.toString()} 
	 * @param nullable capability to store null values
	 * 
	 * @return created column specification
	 */
	public ColumnSpec addColumn(String name,String columntype, boolean nullable)
	{
		ColumnSpec columnSpec = new ColumnSpec(this,name,columntype,nullable);
		this.columnList.add(columnSpec);
		return columnSpec;
	}
	
	/**
	 * create and append a column specification
	 * 
	 * @param name column-name
	 * @param columntype string represent of {@link IColumnType}, for example {@code IColumnType.ColumnType.VARCHAR.toString()} 
	 * @param nullable capability to store null values
	 * @param size for text/char types the length of column
	 * 
	 * @return created column specification
	 */
	public ColumnSpec addColumn(String name,String columntype, boolean nullable, int size)
	{
		ColumnSpec columnSpec = new ColumnSpec(this,name,columntype,nullable,size);
		this.columnList.add(columnSpec);
		return columnSpec;
	}
	
	/**
	 * create and append a index specification
	 * 
	 * @param indexName name of index
	 * @param column affected column
	 * 
	 * @return created index specification
	 */
	public IndexSpec addColumnIndex(String indexName, String column)
	{
		ColumnSpec columnSpec = getColumn(column);
		if(columnSpec == null)
		{
			throw new NullPointerException("TableSpec.addColumnIndex: Column " + column + " not found in table  " + this.name);
		}
		IndexSpec index = new IndexSpec(this, indexName, columnSpec);
		this.columnIndexList.add(index);
		return index;
	}
	
	/**
	 * create and append a index specification
	 * 
	 * @param indexName name of index
	 * @param column affected column
	 * @param unique if true, index is created as unique index
	 * 
	 * @return created index specification
	 */
	public IndexSpec addColumnIndex(String indexName, String column, boolean unique)
	{
		ColumnSpec columnSpec = getColumn(column);
		if(columnSpec == null)
		{
			throw new NullPointerException("TableSpec.addColumnIndex: Column " + column + " not found in table  " + this.name);
		}
		IndexSpec index = new IndexSpec(this, indexName, columnSpec, unique);
		this.columnIndexList.add(index);
		return index;
	}
	
	/**
	 * create and append a index specification
	 * 
	 * @param indexName name of index
	 * @param column affected column
	 * @param unique if true, index is created as unique index
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 * 
	 * @return created index specification
	 */
	public IndexSpec addColumnIndex(String indexName, String column, boolean unique, boolean includeContext)
	{
		ColumnSpec columnSpec = getColumn(column);
		if(columnSpec == null)
		{
			throw new NullPointerException("TableSpec.addColumnIndex: Column " + column + " not found in table  " + this.name);
		}
		IndexSpec index = new IndexSpec(this, indexName, columnSpec, unique,includeContext);
		this.columnIndexList.add(index);
		return index;
	}
	
	/**
	 * create and append a index specification
	 * 
	 * @param indexName name of index
	 * @param columns list of effected columns
	 * @param unique if true, index is created as unique index
	 * 
	 * @return created index specification
	 */
	public IndexSpec addColumnIndex(String indexName, String[] columns, boolean unique)
	{
		List<ColumnSpec> columnList = new ArrayList<ColumnSpec>();
		for(String column : columns)
		{
			ColumnSpec columnSpec = getColumn(column);
			if(columnSpec == null)
			{
				throw new NullPointerException("TableSpec.addColumnIndex: Column " + column + " not found in table  " + this.name);
			}
			columnList.add(columnSpec);
		}
		if(columnList.isEmpty())
		{
			throw new RuntimeException("TableSpec.addColumnIndex: Columns not defined ");
		}
		IndexSpec index = new IndexSpec(this, indexName, columnList, unique);
		this.columnIndexList.add(index);
		return index;
	}
	
	/**
	 * create and append a index specification
	 * 
	 * @param indexName name of index
	 * @param columns list of effected columns
	 * @param unique if true, index is created as unique index
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 * @return
	 */
	public IndexSpec addColumnIndex(String indexName, String[] columns, boolean unique, boolean includeContext)
	{
		List<ColumnSpec> columnList = new ArrayList<ColumnSpec>();
		for(String column : columns)
		{
			ColumnSpec columnSpec = getColumn(column);
			if(columnSpec == null)
			{
				throw new NullPointerException("TableSpec.addColumnIndex: Column " + column + " not found in table  " + this.name);
			}
			columnList.add(columnSpec);
		}
		if(columnList.isEmpty())
		{
			throw new RuntimeException("TableSpec.addColumnIndex: Columns not defined ");
		}
		IndexSpec index = new IndexSpec(this, indexName, columnList, unique, includeContext);
		this.columnIndexList.add(index);
		return index;
	}

	/**
	 * getter for name of table
	 * 
	 * @return name of table
	 */
	public String getName() 
	{
		return name;
	}

	/**
	 * getter for columns of table
	 * 
	 * @return list of columns
	 */
	public List<ColumnSpec> getColumnList() 
	{
		return columnList;
	}
	
	/**
	 * find and return column with given {@code name}
	 * 
	 * @param name name of column to return
	 * 
	 * @return column specification
	 */
	public ColumnSpec getColumn(String name)
	{
		for(ColumnSpec column : this.columnList)
		{
			if(column.getName().equals(name))
			{
				return column;
			}
		}
		return null;
	}

	/**
	 * return true, if column with given name exists in table specification
	 * 
	 * @param name column-name of column to be tested
	 * 
	 * @return true, if column with given name exists, otherwise false
	 */
	public boolean containsColumns(String name) 
	{
		for(ColumnSpec column : this.columnList)
		{
			if(column.getName().equals(name))
			{
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Apply a {@link ITableTemplate} to create a standard base of table specification
	 * 
	 * @param tableTemplateClass class of {@link ITableTemplate}
	 * @return table specification
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public TableSpec applyTemplate(Class<?> tableTemplateClass) throws InstantiationException, IllegalAccessException
	{
		ITableTemplate instanceOfTemplate = (ITableTemplate)tableTemplateClass.newInstance();
		instanceOfTemplate.tableTemplateApply(this);
		return this;
	}

	/**
	 * Getter for quoted name property. If table name is quoted, the name is managed case-sensitive and can include keywords.
	 *  
	 * @return quoted name property
	 */
	public Boolean getQuotedName()
	{
		return quotedName;
	}

	/**
	 * Setter for quoted name property. If table name is quoted, the name is managed case-sensitive and can include keywords.
	 * 
	 * @param quotedName quoted name property
	 * @return table specification
	 */
	public TableSpec setQuotedName(Boolean quotedName)
	{
		this.quotedName = quotedName;
		return this;
	}

	/**
	 * Getter for used tablespace. If dbms supports tablespaces, the table will be created in given tablespace. The tablespace must exist. Once the table is created, the tablespace will not be adjusted anymore.
	 *
	 * @return name of tablespace
	 */
	public String getTableSpace()
	{
		return tableSpace;
	}

	/**
	 * Setter for used tablespace. If dbms supports tablespaces, the table will be created in given tablespace. The tablespace must exist. Once the table is created, the tablespace will not be adjusted anymore.
	 * 
	 * @param tableSpace name of tablespace
	 * @return table specification
	 */
	public TableSpec setTableSpace(String tableSpace)
	{
		this.tableSpace = tableSpace;
		return this;
	}

	/**
	 * Getter for indices
	 * 
	 * @return List of indices
	 */
	public	List<IndexSpec> getColumnIndexList() 
	{
		return columnIndexList;
	}
	
	/**
	 * Getter for dbms schema-name. By default, the schema-name is domain-name. 
	 * 
	 * @return dbms schema-name
	 */
	public String getDbmsSchemaName()
	{
		return dbmsSchemaName;
	}
	
	/**
	 * Setter for dbms schema-name. By default, the schema-name is domain-name. 
	 * 
	 * @param dbmsSchemaName dbms schema-name
	 * 
	 * @return table specification
	 */
	public TableSpec setDbmsSchemaName(String dbmsSchemaName)
	{
		this.dbmsSchemaName = dbmsSchemaName;
		return this;
	}

	/**
	 * getter for parent schema specification with better expression for java fluent api
	 * 
	 * @return parent schema specification
	 */
	public SchemaSpec endTableSpec()
	{
		return this.schema;
	}

	/**
	 * getter for parent schema specification
	 * 
	 * @return parent schema object
	 */
	public SchemaSpec getSchemaSpec()
	{
		return schema;
	}

	/**
	 * Getter for list of {@link IDatabaseSchemaUpdateListener}. SchemaUpdateListeners are informed about the progress of the schema update process. 
	 * 
	 * @return list of schemaUpdateListeners
	 */
	public List<IDatabaseSchemaUpdateListener> getUpdateListenerList()
	{
		return updateListenerList;
	}
	
	/**
	 * Add {@link IDatabaseSchemaUpdateListener} to table, if not already exist. SchemaUpdateListeners are informed about the progress of the schema update process. 
	 * 
	 * @param updateListener the listener being informed about changes regarding this table
	 * 
	 * @return table specification
	 */
	public TableSpec addUpdateListener(IDatabaseSchemaUpdateListener updateListener)
	{
		if(this.updateListenerList == null)
		{
			this.updateListenerList = new ArrayList<IDatabaseSchemaUpdateListener>();
		}
		if(this.updateListenerList.contains(updateListener))
		{
			return this;
		}
		this.updateListenerList.add(updateListener);
		return this;
	}


	@Override
	public String toString()
	{
		return "TableSpec " + name;
	}
	
	
}
