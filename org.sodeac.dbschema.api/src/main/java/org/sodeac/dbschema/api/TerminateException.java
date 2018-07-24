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
 * This exception should thrown, if process {@link IDatabaseSchemaProcessor#checkSchemaSpec(SchemaSpec, java.sql.Connection)} have to stop to work. 
 * Furthermore, if cause is instance of {@link SchemaUnusableException}, the processor returns false in {@link IDatabaseSchemaProcessor#checkSchemaSpec(SchemaSpec, java.sql.Connection)}.
 * 
 * @author Sebastian Palarus
 *
 */
public class TerminateException extends RuntimeException
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4392864023695139818L;

	public TerminateException()
	{
		super();
	}

	public TerminateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TerminateException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public TerminateException(String message)
	{
		super(message);
	}

	public TerminateException(Throwable cause)
	{
		super(cause);
	}

}
