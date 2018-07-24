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
package org.sodeac.dbschema.itest;

import static org.ops4j.pax.exam.CoreOptions.bundle;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.ops4j.pax.exam.options.ProvisionOption;
import org.ops4j.pax.exam.util.PathUtils;
import org.osgi.framework.Bundle;

public class TestTools
{
	public static ProvisionOption<?> reactorBundle(String artifactId, String version) 
	{
		String fileName = String.format("%s/../%s/target/%s-%s.jar", PathUtils.getBaseDir(), artifactId, artifactId,version);

		if (new File(fileName).exists()) 
		{
			try
			{
				String url = "file:" + new File(fileName).getCanonicalPath();
				return bundle(url);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			fileName = String.format("%s/../%s/target/%s-%s-SNAPSHOT.jar", PathUtils.getBaseDir(), artifactId, artifactId,version);

			if (new File(fileName).exists()) 
			{
				try
				{
					String url = "file:" + new File(fileName).getCanonicalPath();
					return bundle(url);
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	public static String getBundleStateName(int state)
	{
		switch (state) 
		{
			case Bundle.UNINSTALLED:
					
				return "UNINSTALLED";
				
			case Bundle.INSTALLED:
				
				return "INSTALLED";
	
			case Bundle.RESOLVED:
				
				return "RESOLVED";
			
			case Bundle.STARTING:
				
				return "STARTING";
			
			case Bundle.STOPPING:
				
				return "STOPPING";
				
			case Bundle.ACTIVE:
				
				return "ACTIVE";
			default:
				
				return "State " + state;
		}
	}
	
	public static String getSchemaName()
	{
		try
		{
			Date begin = new SimpleDateFormat("yyyyMMddHHmmssSSS").parse("20180101000000000");
			Date now = new Date();
			long diff = now.getTime() - begin.getTime();
			diff = diff / 1000;
			Thread.sleep(2000);
			return String.format("%09X", diff);
		}
		catch (Exception e) 
		{
			if(e instanceof RuntimeException)
			{
				throw (RuntimeException)e;
			}
			throw new RuntimeException(e);
		}
	}
}
