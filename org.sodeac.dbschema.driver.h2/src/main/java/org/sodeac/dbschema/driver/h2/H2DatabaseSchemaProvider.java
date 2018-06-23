package org.sodeac.dbschema.driver.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.driver.base.DefaultDatabaseSchemaDriver;


@Component(service=IDatabaseSchemaDriver.class)
public class H2DatabaseSchemaProvider extends DefaultDatabaseSchemaDriver implements IDatabaseSchemaDriver
{
	@Override
	public int handle(Connection connection) throws SQLException
	{
		if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("H2"))
		{
			return IDatabaseSchemaDriver.HANDLE_DEFAULT;
		}
		return IDatabaseSchemaDriver.HANDLE_NONE;
	}
	
	@Override
	public void setPrimaryKey
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		Map<String, Object> tableProperties
	) throws SQLException
	{
		super.setPrimaryKeyWithIndex(connection, schemaSpec, tableSpec, tableProperties);
	}
	
	@Override
	public String objectNameGuidelineFormat(SchemaSpec schemaSpec, Connection connection, String name, String type)
	{
		return name == null ? name : name.toUpperCase();
	}
}
