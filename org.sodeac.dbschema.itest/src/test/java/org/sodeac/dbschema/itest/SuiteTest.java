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
