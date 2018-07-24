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
package org.sodeac.dbschema.impl;

import java.util.ArrayList;
import java.util.List;

import org.sodeac.dbschema.api.SchemaUnusableException;

public class CheckProperties
{
	private boolean interrupted = false;
	private List<SchemaUnusableException> unusableExceptionList = new ArrayList<SchemaUnusableException>();

	public boolean isInterrupted()
	{
		return interrupted;
	}
	public void setInterrupted(boolean interrupted)
	{
		this.interrupted = interrupted;
	}
	
	public List<SchemaUnusableException> getUnusableExceptionList()
	{
		return unusableExceptionList;
	}
	public void setUnusableExceptionList(List<SchemaUnusableException> unusableExceptionList)
	{
		this.unusableExceptionList = unusableExceptionList;
	}
}
