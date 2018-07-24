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

/**
 * Interface to create default database objects in schema
 * 
 * @author Sebastian Palarus
 *
 */
public interface ISchemaTemplate
{

	/**
	 * implementation of applying schemaTemplate to schemaSpec
	 * 
	 * @param schemaSpec
	 */
	public void schemaTemplateApply(SchemaSpec schemaSpec);
}
