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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses
        ({
                DBSchemaIT.class,
                DBSchemaTableIT.class,
                DBSchemaColumnIT.class,
                DBSchemaColumnTypeTextIT.class,
                DBSchemaColumnTypeIntegerIT.class,
                DBSchemaColumnTypeDecimalIT.class,
                DBSchemaColumnTypeTimeIT.class,
                DBSchemaColumnTypeBinaryIT.class,
                DBSchemaKeysIT.class,
                DBSchemaColumnPropertiesIT.class,
                DBSchemaDisableChecksIT.class,
                DBSchemaTableTemplateIT.class
        })
public class SuiteTests
{

}
