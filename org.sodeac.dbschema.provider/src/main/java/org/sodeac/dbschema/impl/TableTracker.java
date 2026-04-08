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
import java.util.Map;

import org.sodeac.dbschema.api.TableSpec;

public class TableTracker
{
    private TableSpec tableSpec = null;
    private boolean created = false;
    private boolean exits = false;
    private Map<String, Object> tableProperties = null;
    private final List<ColumnTracker> columnTrackerList = new ArrayList<ColumnTracker>();
    
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
    
    public Map<String, Object> getTableProperties()
    {
        return this.tableProperties;
    }
    
    public void setTableProperties(final Map<String, Object> tableProperties)
    {
        this.tableProperties = tableProperties;
    }
    
    public TableSpec getTableSpec()
    {
        return this.tableSpec;
    }
    
    public void setTableSpec(final TableSpec tableSpec)
    {
        this.tableSpec = tableSpec;
    }
    
    public List<ColumnTracker> getColumnTrackerList()
    {
        return this.columnTrackerList;
    }
}
