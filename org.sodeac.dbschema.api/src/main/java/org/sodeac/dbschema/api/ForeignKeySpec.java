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
 * specification of foreign key
 * 
 * @author Sebastian Palarus
 *
 */
public class ForeignKeySpec 
{
	private String constraintName = null;
	private String referencedTableName = null;
	private String referencedColumnName = DatabaseCommonElements.ID;
	private Boolean quotedKeyName = null;
	private Boolean quotedRefTableName = null;
	private Boolean quotedRefColumnName = null;
	
	/**
	 * Creates new foreignkey-specification with {@code constraintName} and {@code referencedTableName} and referenced column {@link DatabaseCommonElements#ID}
	 * 
	 * @param constraintName name of foreignkey
	 * @param referencedTableName name of referenced table
	 */
	public ForeignKeySpec(String constraintName,String referencedTableName)
	{
		super();
		this.constraintName = constraintName;
		this.referencedTableName = referencedTableName;
	}
	
	/**
	 * Creates new foreignkey-specification with {@code constraintName} and {@code referencedTableName} and {@code referencedColumnName}
	 * 
	 * @param constraintName name of foreign key
	 * @param referencedTableName name of referenced table
	 * @param referencedColumnName name of referenced column
	 */
	public ForeignKeySpec(String constraintName,String referencedTableName,String referencedColumnName)
	{
		super();
		this.constraintName = constraintName;
		this.referencedTableName = referencedTableName;
		this.referencedColumnName = referencedColumnName;
	}
	
	/**
	 * Getter for name of foreign key
	 * 
	 * @return name of foreign key
	 */
	public String getConstraintName() 
	{
		return constraintName;
	}
	
	/**
	 * Setter for name of foreign key
	 * 
	 * @param constraintName name of foreign key
	 * 
	 * @return foreign key specification
	 */
	public ForeignKeySpec setConstraintName(String constraintName) 
	{
		this.constraintName = constraintName;
		return this;
	}
	
	/**
	 * Getter for name of referenced table
	 * 
	 * @return name of referenced table
	 */
	public String getTableName() 
	{
		return referencedTableName;
	}
	
	/**
	 * Setter for name of referenced table
	 * 
	 * @param referencedTableName name of referenced table
	 * 
	 * @return foreign key specification
	 */
	public ForeignKeySpec setReferencedTableName(String referencedTableName) 
	{
		this.referencedTableName = referencedTableName;
		return this;
	}
	
	/**
	 * Getter for referenced column name
	 * 
	 * @return referenced column name
	 */
	public String getReferencedColumnName() 
	{
		return referencedColumnName;
	}
	
	/**
	 * Setter for feferenced column name
	 * 
	 * @param referencedColumnName referenced column name
	 * 
	 * @return foreign key specification
	 */
	public ForeignKeySpec setReferencedColumnName(String referencedColumnName) 
	{
		this.referencedColumnName = referencedColumnName;
		return this;
	}
	
	/**
	 * Getter for quoted-ref-table-name property
	 * 
	 * @return quoted-ref-table-name property
	 */
	public Boolean getQuotedRefTableName()
	{
		return quotedRefTableName;
	}
	
	/**
	 * Setter for quoted-ref-table-name property
	 * 
	 * @param quotedRefTableName quoted-ref-table-name property
	 * @return
	 */
	public ForeignKeySpec setQuotedRefTableName(Boolean quotedRefTableName)
	{
		this.quotedRefTableName = quotedRefTableName;
		return this;
	}
	
	/**
	 * Getter for quoted-ref-column-name property
	 * 
	 * @return quoted-ref-column-name property
	 */
	public Boolean getQuotedRefColumnName()
	{
		return quotedRefColumnName;
	}
	
	/**
	 * Setter for quoted-ref-column-name property
	 * 
	 * @param quotedRefColumnName quoted-ref-column-name property
	 * 
	 * @return foreign key specification
	 */
	public ForeignKeySpec setQuotedRefColumnName(Boolean quotedRefColumnName)
	{
		this.quotedRefColumnName = quotedRefColumnName;
		return this;
	}
	
	/**
	 * Getter for quoted name property. If foreign key name is quoted, the name is managed case-sensitive and can include keywords.
	 *  
	 * @return quoted name property
	 */
	public Boolean getQuotedKeyName()
	{
		return quotedKeyName;
	}
	
	/**
	 * Setter for quoted name property. If foreign key name is quoted, the name is managed case-sensitive and can include keywords.
	 * 
	 * @param quotedName quoted name property
	 * @return foreign key specification
	 */
	public ForeignKeySpec setQuotedKeyName(Boolean quotedName)
	{
		this.quotedKeyName = quotedName;
		return this;
	}
}
