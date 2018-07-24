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

/**
 * primary key specification
 * 
 * @author Sebastian Palarus
 *
 */
public class PrimaryKeySpec 
{
	private String indexName = null;
	private String constraintName = null;
	private Boolean quotedName = null;
	private String tableSpace = null;
	
	/**
	 * Creates new primarykey-specification with {@code constraintName} and {@code indexName}
	 * 
	 * @param constraintName name of foreignkey
	 * @param indexName name of index
	 */
	public PrimaryKeySpec(String constraintName, String indexName)
	{
		super();
		this.indexName = indexName;
		this.constraintName = constraintName;
	}
	
	/**
	 * Getter for index name
	 * 
	 * @return index name
	 */
	public String getIndexName() 
	{
		return indexName;
	}
	
	/**
	 * Setter for index name 
	 * 
	 * @param indexName name of index
	 * @return primary key specification
	 */
	public PrimaryKeySpec setIndexName(String indexName) 
	{
		this.indexName = indexName;
		return this;
	}
	
	/**
	 * Getter for primary key name
	 * 
	 * @return primary key name
	 */
	public String getConstraintName()
	{
		return constraintName;
	}

	/**
	 * Setter for primary key name
	 * 
	 * @param constraintName primary  key name
	 * 
	 * @return primary key specification
	 */
	public PrimaryKeySpec setConstraintName(String constraintName)
	{
		this.constraintName = constraintName;
		return this;
	}

	/**
	 * Getter for quoted name property. If primary key names is quoted, the names (constraint name and index name) is managed case-sensitive and can include keywords.
	 *  
	 * @return quoted name property (constraint name and index name)
	 */
	public Boolean getQuotedName()
	{
		return quotedName;
	}

	/**
	 * Setter for quoted name property. If primary key names is quoted, the names (constraint name and index name) is managed case-sensitive and can include keywords.
	 * 
	 * @param quotedName quoted name property (constraint name and index name)
	 * 
	 * @return primary key specification
	 */
	public PrimaryKeySpec setQuotedName(Boolean quotedName)
	{
		this.quotedName = quotedName;
		return this;
	}

	/**
	 * Getter for used tablespace. If dbms supports tablespaces, the primary key will be created in given tablespace. The tablespace must exist. Once the primary is created, the tablespace will not be adjusted anymore.
	 *
	 * @return name of tablespace
	 */
	public String getTableSpace()
	{
		return tableSpace;
	}

	/**
	 * Setter for used tablespace. If dbms supports tablespaces, the primary will be created in given tablespace. The tablespace must exist. Once the primary key is created, the tablespace will not be adjusted anymore.
	 * 
	 * @param tableSpace name of tablespace
	 * @return primary key specification
	 */
	public PrimaryKeySpec setTableSpace(String tableSpace)
	{
		this.tableSpace = tableSpace;
		return this;
	}
}
