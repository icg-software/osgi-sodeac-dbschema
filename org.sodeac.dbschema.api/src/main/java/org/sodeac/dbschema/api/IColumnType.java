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
package org.sodeac.dbschema.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Interface for column type driver implementations
 * 
 * @author Sebastian Palarus
 *
 */
public interface IColumnType
{
	/**
	 * Embedded column types
	 *
	 */
	public static enum ColumnType {CHAR,VARCHAR,CLOB,BOOLEAN,SMALLINT,INTEGER,BIGINT,REAL,DOUBLE,TIMESTAMP,DATE,TIME,BINARY,BLOB}
	
	/**
	 * Priority value for driver
	 * 
	 */
	public static enum Applicability{NONE,FALLBACK,STANDARD,SPECIFIC};
	
	/**
	 * Getter for supported column types
	 * 
	 * @return list of string represent of supported column types
	 */
	public List<String> getTypeList();
	
	/**
	 * checks the applicability of this driver to column type
	 * 
	 * @param connection used connection
	 * @param schemaSpec used schema specification
	 * @param tableSpec used table specification
	 * @param columnSpec used column specification
	 * @param dbProduct database product name
	 * @param schemaDriver used schema driver
	 * 
	 * @return applicability
	 * 
	 * @throws SQLException
	 */
	public default Applicability getApplicability(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, String dbProduct, IDatabaseSchemaDriver schemaDriver) throws SQLException {return Applicability.STANDARD;}
	
	/**
	 * return the expression for column type in create or alter column command
	 * 
	 * @param connection used connection
	 * @param schemaSpec used schema specification
	 * @param tableSpec used table specification
	 * @param columnSpec used column specification
	 * @param dbProduct database product name
	 * @param schemaDriver used schema driver
	 * 
	 * @return expression for column type in create or alter column command
	 * 
	 * @throws SQLException
	 */
	public String getTypeExpression(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, String dbProduct, IDatabaseSchemaDriver schemaDriver) throws SQLException;
	
	/**
	 * return the expression for default type of column in create or alter column command
	 * 
	 * @param connection used connection
	 * @param schemaSpec used schema specification
	 * @param tableSpec used table specification
	 * @param columnSpec used column specification
	 * @param dbProduct database product name
	 * @param schemaDriver used schema driver
	 * @return expression for default type of column in create or alter column command
	 * @throws SQLException
	 */
	public String getDefaultValueExpression(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, String dbProduct, IDatabaseSchemaDriver schemaDriver) throws SQLException;
}
