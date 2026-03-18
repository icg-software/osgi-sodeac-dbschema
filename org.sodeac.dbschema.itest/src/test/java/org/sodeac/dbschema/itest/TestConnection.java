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

import java.io.Serializable;
import java.sql.Connection;

import lombok.ToString;

@ToString
public class TestConnection implements Serializable
{
    private static final long serialVersionUID = 1L;

    public transient Connection connection;
    public boolean enabled = false;
    public String dbmsSchemaName = null;
    public String tableSpaceIndex = null;
    public String tableSpaceData = null;

    public TestConnection()
    {
        super();
    }

    public TestConnection(final boolean enabled)
    {
        super();
        this.enabled = enabled;
    }
}
