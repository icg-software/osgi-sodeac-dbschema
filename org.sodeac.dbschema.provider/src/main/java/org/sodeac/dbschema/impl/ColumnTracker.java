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

import java.util.Map;

import org.sodeac.dbschema.api.ColumnSpec;

public class ColumnTracker
{
    private ColumnSpec columnSpec = null;
    private boolean created = false;
    private boolean exits = false;
    private Map<String, Object> columnProperties = null;
    
    public boolean isCreated()
    {
        return this.created;
    }
    
    public void setCreated(final boolean created)
    {
        this.created = created;
    }
    
    public boolean isExits()
    {
        return this.exits;
    }
    
    public void setExits(final boolean exits)
    {
        this.exits = exits;
    }
    
    public ColumnSpec getColumnSpec()
    {
        return this.columnSpec;
    }
    
    public void setColumnSpec(final ColumnSpec columnSpec)
    {
        this.columnSpec = columnSpec;
    }
    
    public Map<String, Object> getColumnProperties()
    {
        return this.columnProperties;
    }
    
    public void setColumnProperties(final Map<String, Object> columnProperties)
    {
        this.columnProperties = columnProperties;
    }
}
