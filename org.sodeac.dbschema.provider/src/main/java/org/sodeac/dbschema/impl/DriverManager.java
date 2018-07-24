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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.osgi.framework.ServiceReference;

public class DriverManager<T>
{
	public DriverManager()
	{
		super();
		this.lock = new ReentrantLock(true);
	}
	
	private Lock lock = null;
	private volatile List<T> driverList = new ArrayList<T>();
	private List<Reference> referenceList = new ArrayList<Reference>();
	
	public void add(T service, ServiceReference<T> serviceReference)
	{
		lock.lock();
		try
		{
			Reference reference = new Reference();
			reference.service = service;
			reference.serviceReference = serviceReference;
			this.referenceList.add(reference);
			
			reCreateDriverList();
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	public void remove(T service, ServiceReference<T> serviceReference)
	{
		lock.lock();
		try
		{
			List<Reference> toRemove = new ArrayList<Reference>();
			for(Reference ref : referenceList)
			{
				if(ref == service)
				{
					toRemove.add(ref);
				}
			}
			for(Reference rm : toRemove)
			{
				System.out.println("RM " + rm.service);
				referenceList.remove(rm);
			}
			reCreateDriverList();
		}
		finally 
		{
			lock.unlock();
		}
	}
	
	
	private void reCreateDriverList()
	{
		Map<String,Map<String,List<Reference>>> structuredReferences = new HashMap<String,Map<String,List<Reference>>>();
		
		for(Reference reference : referenceList)
		{
			String className = reference.service.getClass().getCanonicalName();
			String bundleName = reference.serviceReference.getBundle().getSymbolicName();
			
			Map<String,List<Reference>> indexByName = structuredReferences.get(className);
			if(indexByName == null)
			{
				indexByName = new HashMap<String,List<Reference>>();
				structuredReferences.put(className,indexByName);
			}
			List<Reference> list = indexByName.get(bundleName);
			if(list == null)
			{
				list = new ArrayList<Reference>();
				indexByName.put(bundleName, list);
			}
			list.add(reference);
		}
		
		List<T> newDriverList = new ArrayList<>();
		
		for(Entry<String,Map<String,List<Reference>>> entry1 : structuredReferences.entrySet())
		{
			for(Entry<String,List<Reference>> entry2 : entry1.getValue().entrySet())
			{
				Reference highest = null;
				for(Reference current : entry2.getValue())
				{
					if(highest == null)
					{
						highest = current;
						continue;
					}
					if(current.serviceReference.getBundle().getVersion().compareTo(highest.serviceReference.getBundle().getVersion()) > 0)
					{
						highest = current;
					}
				}
				if(highest != null)
				{
					newDriverList.add(highest.service);
				}
			}
		}
		
		this.driverList = newDriverList;
	}
	
	private class Reference
	{
		private T service = null;
		private ServiceReference<T> serviceReference = null;
	}

	public List<T> getDriverList()
	{
		return driverList;
	}
}
