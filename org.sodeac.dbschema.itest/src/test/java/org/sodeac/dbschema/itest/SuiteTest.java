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
	DBSchema.class,
	DBSchemaTable.class,
	DBSchemaColumn.class,
	DBSchemaColumnTypeText.class,
	DBSchemaColumnTypeInteger.class,
	DBSchemaColumnTypeDecimal.class,
	DBSchemaColumnTypeTime.class,
	DBSchemaColumnTypeBinary.class,
	DBSchemaKeys.class,
	DBSchemaColumnProperties.class,
	DBSchemaDisableChecks.class,
	DBSchemaTableTemplate.class
})
public class SuiteTest
{

}
