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
 * index specification
 * 
 * @author Sebastian Palarus
 *
 */
public class IndexSpec 
{
	/**
	 * Creates new index-specification for  a {@code tableSpec} with {@code indexName} and one affected {@code column}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param column affected column
	 */
	public IndexSpec(TableSpec tableSpec,String indexName, ColumnSpec column)
	{
		this(tableSpec,indexName,column,false);
	}
	
	/**
	 * Creates new index-specification for  a {@code tableSpec} with {@code indexName} and one affected {@code columns}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param columns affected columns
	 */
	public IndexSpec(TableSpec tableSpec,String indexName, List<ColumnSpec> columns)
	{
		this(tableSpec,indexName,columns,false);
	}
	
	/**
	 * Creates new index-specification for  a {@code tableSpec} with {@code indexName} and one affected {@code column}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param column affected column
	 * @param unique if true, index is unique, otherwise not
	 */
	public IndexSpec(TableSpec tableSpec,String indexName, ColumnSpec column, boolean unique)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = new ArrayList<ColumnSpec>();
		this.columns.add(column);
		this.unique = unique;
	}
	
	/**
	 * Creates new index-specification for a {@code tableSpec} with {@code indexName} and affected {@code column}, optionally combined with column {@link DatabaseCommonElements#CONTEXT}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param column affected column
	 * @param unique if true, index is unique, otherwise not
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 */
	public IndexSpec(TableSpec tableSpec,String indexName, ColumnSpec column, boolean unique, boolean includeContext)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = new ArrayList<ColumnSpec>();
		this.columns.add(column);
		this.unique = unique;
		this.includeContext = includeContext;
	}
	
	/**
	 * Creates new index-specification for a {@code tableSpec} with {@code indexName} and a list of affected {@code columns}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param columns affected columns
	 * @param unique if true, index is unique, otherwise not
	 */
	public IndexSpec(TableSpec tableSpec, String indexName, List<ColumnSpec> columns, boolean unique)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = columns;
		if(this.columns == null)
		{
			this.columns = new ArrayList<ColumnSpec>();
		}
		this.unique = unique;
	}
	
	/**
	 * Creates new index-specification for a {@code tableSpec} with {@code indexName} and an array of affected {@code columns}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param columns affected columns
	 * @param unique if true, index is unique, otherwise not
	 */
	public IndexSpec(TableSpec tableSpec, String indexName, ColumnSpec[] columns, boolean unique)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = new ArrayList<ColumnSpec>();
		for(int i = 0; i < columns.length; i++)
		{
			this.columns.add(columns[i]);
		}
		this.unique = unique;
	}
	
	/**
	 * Creates new index-specification for a {@code tableSpec} with {@code indexName} and a list of affected {@code columns}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param columns affected columns
	 * @param unique if true, index is unique, otherwise not
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 */
	public IndexSpec(TableSpec tableSpec, String indexName, List<ColumnSpec> columns, boolean unique, boolean includeContext)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = columns;
		if(this.columns == null)
		{
			this.columns = new ArrayList<ColumnSpec>();
		}
		this.unique = unique;
	}
	
	/**
	 * Creates new index-specification for a {@code tableSpec} with {@code indexName} and an array of affected {@code columns}
	 * 
	 * @param tableSpec table specification
	 * @param indexName name of index
	 * @param columns affected columns
	 * @param unique if true, index is unique, otherwise not
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 */
	public IndexSpec(TableSpec tableSpec, String indexName, ColumnSpec[] columns, boolean unique, boolean includeContext)
	{
		super();
		this.tableSpec = tableSpec;
		this.indexName = indexName;
		this.columns = new ArrayList<ColumnSpec>();
		for(int i = 0; i < columns.length; i++)
		{
			this.columns.add(columns[i]);
		}
		this.unique = unique;
	}
	
	private boolean unique = false;
	private String indexName = null;
	private List<ColumnSpec> columns = null;
	private boolean includeContext = false;
	private TableSpec tableSpec = null;
	private Boolean quotedName = null;
	private String tableSpace = null;
	
	/**
	 * Setter for include context property
	 * 
	 * @param includeContext if true, column {@link DatabaseCommonElements#CONTEXT} is a member of index
	 * 
	 * @return index specification
	 */
	public IndexSpec setIncludeContext(boolean includeContext) 
	{
		this.includeContext = includeContext;
		return this;
	}

	/**
	 * Getter for unique-property
	 * 
	 * @return true, if index is unique, otherwise false
	 */
	public boolean getUnique() 
	{
		return unique;
	}

	/**
	 * Getter for index name
	 * 
	 * @return name of index
	 */
	public String getIndexName() 
	{
		return indexName;
	}

	/**
	 * Getter for quoted name property. If index name is quoted, the name is managed case-sensitive and can include keywords.
	 * 
	 * @return true, if name of index is quoted
	 */
	public Boolean getQuotedName()
	{
		return quotedName;
	}

	/**
	 * Setter for quoted name property. If index name is quoted, the name is managed case-sensitive and can include keywords.
	 * 
	 * @param quotedName quoted name property
	 * 
	 * @return index specification
	 */
	public IndexSpec setQuotedName(Boolean quotedName)
	{
		this.quotedName = quotedName;
		return this;
	}

	/**
	 * Getter for tablespace. If dbms supports tablespaces, the index will be created in given tablespace. The tablespace must exist. Once the index is created, the tablespace will not be adjusted anymore.
	 * 
	 * @return name tablespace
	 */
	public String getTableSpace()
	{
		return tableSpace;
	}

	/**
	 * Setter for tablespace. If dbms supports tablespaces, the index will be created in given tablespace. The tablespace must exist. Once the index is created, the tablespace will not be adjusted anymore.
	 * 
	 * @param tableSpace name of tablespace
	 * 
	 * @return index specification
	 */
	public IndexSpec setTableSpace(String tableSpace)
	{
		this.tableSpace = tableSpace;
		return this;
	}

	/**
	 * Getter for columnlist
	 * 
	 * @return list of columns
	 */
	public List<ColumnSpec> getColumns() 
	{
		return columns;
	}
	
	/**
	 * Getter for include context property
	 * 
	 * @return include context property
	 */
	public boolean getIncludeContext()
	{
		return includeContext;
	}
	
	
	/**
	 * Getter for parent table specification
	 * 
	 * @return table specification
	 */
	public TableSpec getTableSpec()
	{
		return tableSpec;
	}

	/**
	 * Getter for parent table specification with better expression for java fluent api
	 * 
	 * @return table specification
	 */
	public TableSpec endColumnIndexDefinition()
	{
		return this.tableSpec;
	}

	@Override
	public String toString()
	{
		return "IndexSpec " + indexName;
	}
}
