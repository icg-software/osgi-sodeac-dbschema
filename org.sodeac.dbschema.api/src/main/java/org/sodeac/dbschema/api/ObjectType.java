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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * 
 * An object type describes the kind of object (table,column,key ...) in schema update workflow. The types specify the event in  {@link IDatabaseSchemaUpdateListener#onAction(ActionType, ObjectType, PhaseType, java.sql.Connection, String, java.util.Dictionary, IDatabaseSchemaDriver, Exception)}.
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 */
public enum ObjectType 
{
	/**
	 * handle schema
	 */
	SCHEMA(1),
	
	/**
	 * handle table
	 */
	TABLE(2),
	
	/**
	 * handle primary key
	 */
	TABLE_PRIMARY_KEY(5),
	
	/**
	 * handle index
	 */
	TABLE_INDEX(8),
	
	/**
	 * handle column
	 */
	COLUMN(9),
	
	/**
	 * handle default value of column
	 */
	COLUMN_DEFAULT_VALUE(11),
	
	/**
	 * handle nullable option of column
	 */
	COLUMN_NULLABLE(12),
	
	/**
	 * handle size of text (char,varchar) columns
	 */
	COLUMN_SIZE(13),
	
	/**
	 * handle type of column (char,float,blob ...)
	 */
	COLUMN_TYPE(14),
	
	/**
	 * handle foreign key
	 */
	COLUMN_FOREIGN_KEY(15),
	
	/**
	 * handle custom schema-transformation between createing tables/columns and column-properties/keys
	 */
	SCHEMA_CONVERT_SCHEMA(16)
	
	;
	
	private ObjectType(int intValue)
	{
		this.intValue = intValue;
	}
	
	private static volatile Set<ObjectType> ALL = null;
	
	private int intValue;
	
	/**
	 * getter for all object types
	 * 
	 * @return Set of all object types
	 */
	public static Set<ObjectType> getAll()
	{
		if(ObjectType.ALL == null)
		{
			EnumSet<ObjectType> all = EnumSet.allOf(ObjectType.class);
			ObjectType.ALL = Collections.unmodifiableSet(all);
		}
		return ObjectType.ALL;
	}
	
	/**
	 * search object type enum represents by {@code value}
	 * 
	 * @param value integer value of object type
	 * 
	 * @return object type enum represents by {@code value}
	 */
	public static ObjectType findByInteger(int value)
	{
		for(ObjectType actionType : getAll())
		{
			if(actionType.intValue == value)
			{
				return actionType;
			}
		}
		return null;
	}
	
	/**
	 * search object type enum represents by {@code name}
	 * 
	 * @param name of object type
	 * 
	 * @return enum represents by {@code name}
	 */
	public static ObjectType findByName(String name)
	{
		for(ObjectType actionType : getAll())
		{
			if(actionType.name().equalsIgnoreCase(name))
			{
				return actionType;
			}
		}
		return null;
	}
}
