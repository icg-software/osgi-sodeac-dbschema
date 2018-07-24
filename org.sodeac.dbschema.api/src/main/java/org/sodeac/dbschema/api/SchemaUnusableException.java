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
 * This exception should thrown, if schema is unusable. In this case, the process {@link IDatabaseSchemaProcessor#checkSchemaSpec(SchemaSpec, java.sql.Connection)} return false. 
 * 
 * @author Sebastian Palarus
 *
 */
public class SchemaUnusableException extends RuntimeException
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2808225220948944523L;

	public SchemaUnusableException()
	{
		super();
	}

	public SchemaUnusableException(String message, Throwable cause, boolean enableSuppression,boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public SchemaUnusableException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public SchemaUnusableException(String message)
	{
		super(message);
	}

	public SchemaUnusableException(Throwable cause)
	{
		super(cause);
	}
	
}
