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
 * An phase type describes the moment of handle database object in schema update workflow. The types specify the event in  {@link IDatabaseSchemaUpdateListener#onAction(ActionType, ObjectType, PhaseType, java.sql.Connection, String, java.util.Dictionary, IDatabaseSchemaDriver, Exception)}.
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 */
public enum PhaseType 
{
	/**
	 * before checks or update an object
	 */
	PRE(1),
	
	/**
	 * after checks or update an object
	 */
	POST(2);
	
	private PhaseType(int intValue)
	{
		this.intValue = intValue;
	}
	
	private static volatile Set<PhaseType> ALL = null;
	
	private int intValue;
	
	public static Set<PhaseType> getAll()
	{
		if(PhaseType.ALL == null)
		{
			EnumSet<PhaseType> all = EnumSet.allOf(PhaseType.class);
			PhaseType.ALL = Collections.unmodifiableSet(all);
		}
		return PhaseType.ALL;
	}
	
	public static PhaseType findByInteger(int value)
	{
		for(PhaseType actionType : getAll())
		{
			if(actionType.intValue == value)
			{
				return actionType;
			}
		}
		return null;
	}
	
	public static PhaseType findByName(String name)
	{
		for(PhaseType actionType : getAll())
		{
			if(actionType.name().equalsIgnoreCase(name))
			{
				return actionType;
			}
		}
		return null;
	}
}
