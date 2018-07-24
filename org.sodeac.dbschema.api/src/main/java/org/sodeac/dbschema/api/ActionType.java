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
 * An action type describes the kind of process in schema update workflow. The types specify the event in  {@link IDatabaseSchemaUpdateListener#onAction(ActionType, ObjectType, PhaseType, java.sql.Connection, String, java.util.Dictionary, IDatabaseSchemaDriver, Exception)}.
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 */
public enum ActionType 
{
	/**
	 * schema processor checks the existence or validity of an object (table,column, key ...)
	 */
	CHECK(1),
	
	/**
	 * schema processor has to update an object (table,column, key ...)
	 */
	UPDATE(2);
	
	private ActionType(int intValue)
	{
		this.intValue = intValue;
	}
	
	private static volatile Set<ActionType> ALL = null;
	
	private int intValue;
	
	/**
	 * getter for all action types
	 * 
	 * @return Set of all action types
	 */
	public static Set<ActionType> getAll()
	{
		if(ActionType.ALL == null)
		{
			EnumSet<ActionType> all = EnumSet.allOf(ActionType.class);
			ActionType.ALL = Collections.unmodifiableSet(all);
		}
		return ActionType.ALL;
	}
	
	/**
	 * search action type enum represents by {@code value}
	 * 
	 * @param value integer value of action type
	 * 
	 * @return action type enum represents by {@code value}
	 */
	public static ActionType findByInteger(int value)
	{
		for(ActionType actionType : getAll())
		{
			if(actionType.intValue == value)
			{
				return actionType;
			}
		}
		return null;
	}
	
	/**
	 * search action type enum represents by {@code name}
	 * 
	 * @param name of action type
	 * 
	 * @return enum represents by {@code name}
	 */
	public static ActionType findByName(String name)
	{
		for(ActionType actionType : getAll())
		{
			if(actionType.name().equalsIgnoreCase(name))
			{
				return actionType;
			}
		}
		return null;
	}
}
