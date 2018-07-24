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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database Schema Processor to create or update a database schema by specification objects
 * 
 * @author Sebastian Palarus
 *
 */
public interface IDatabaseSchemaProcessor 
{
	/**
	 * determine database schema driver
	 * 
	 * @param connection used connection
	 * 
	 * @return database schema driver
	 * 
	 * @throws SQLException
	 */
	public IDatabaseSchemaDriver getDatabaseSchemaDriver(Connection connection) throws SQLException;
	
	/**
	 * create or update a database schema by specification objects
	 * 
	 * @param schemaSpec schema specification
	 * @param connection used connection
	 * @return false, if process is interrupted, otherwise true
	 * 
	 * @throws SQLException
	 */
	public boolean checkSchemaSpec(SchemaSpec schemaSpec, Connection connection) throws SQLException;
}
