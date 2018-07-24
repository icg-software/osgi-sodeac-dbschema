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
package org.sodeac.dbschema.driver.postgresql;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.driver.base.DefaultDatabaseSchemaDriver;


@Component(service=IDatabaseSchemaDriver.class)
public class PGDatabaseSchemaProvider extends DefaultDatabaseSchemaDriver implements IDatabaseSchemaDriver
{
	// https://jdbc.postgresql.org/documentation/head/binary-data.html
	// https://www.techonthenet.com/postgresql/index.php
	
	@Override
	public int handle(Connection connection) throws SQLException
	{
		if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
		{
			return IDatabaseSchemaDriver.HANDLE_DEFAULT;
		}
		return IDatabaseSchemaDriver.HANDLE_NONE;
	}
	
	@Override
	protected String tableSpaceAppendix
	(
		Connection connection,
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		Map<String, Object> properties,
		String tableSpace,
		String type
	)
	{
		if("PRIMARYKEY".equals(type))
		{
			return  " USING INDEX TABLESPACE " + tableSpace;
		}
		
		return  " TABLESPACE " + tableSpace;
	}
	
	@Override
	public String determineColumnType
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		ColumnSpec columnSpec, Map<String, Object> columnProperties
	) throws SQLException
	{
		if(columnProperties == null)
		{
			return null;
		}
		if(columnProperties.get("COLUMN_TYPE_NAME") == null)
		{
			return null;
		}
		
		if("bpchar".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.CHAR.toString();
		}
		if("bytea".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BINARY.toString();
		}
		if("oid".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BLOB.toString();
		}
		return super.determineColumnType(connection, schemaSpec, tableSpec, columnSpec, columnProperties);
	}
	
	@Override
	public boolean columnExists
	(
		Connection connection, 
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		ColumnSpec columnSpec, 
		Map<String, Object> properties
	) throws SQLException
	{
		boolean columnExists = super.columnExists(connection, schemaSpec, tableSpec, columnSpec, properties);
		String defaultValue = (String)properties.get("COLUMN_COLUMN_DEF");
		if(defaultValue != null)
		{
			// example: 'defaultvalue'::character varying
			
			String[] splitArray = defaultValue.split("::");
			if(splitArray.length == 2)
			{
				if((! splitArray[1].contains("'")) && (! splitArray[0].isEmpty()))
				{
					properties.put("COLUMN_COLUMN_DEF", splitArray[0]);
				}
			}
		}
		return columnExists;
	}
	
	@Override
	public void setValidColumnProperties
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		ColumnSpec columnSpec, Map<String, Object> columnProperties
	) throws SQLException
	{
		String schema = connection.getSchema();
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = objectNameGuidelineFormat(schemaSpec, connection, schemaSpec.getDbmsSchemaName(), "SCHEMA");
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getDbmsSchemaName(), "SCHEMA");
		}
		
		boolean tableQuoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			tableQuoted = tableSpec.getQuotedName().booleanValue();
		}
		
		boolean columnQuoted = false;
		if(columnSpec.getQuotedName() != null)
		{
			columnQuoted = columnSpec.getQuotedName().booleanValue();
		}
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String columnPart =  columnQuoted ? 
				" " + quotedChar() +  "" + columnSpec.getName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getName(), "COLUMN") + " " ;
		
		boolean nullable = columnSpec.getNullable();
		
		if(columnProperties.get("INVALID_NULLABLE") != null)
		{
			PreparedStatement updateNullableStatment = null;
			try
			{
				String nullableStatement = "ALTER TABLE  " + tablePart +  " ALTER COLUMN " + columnPart + " " + ( nullable ? " DROP NOT NULL " : " SET NOT NULL" ) ;
				updateNullableStatment = connection.prepareStatement(nullableStatement);
				updateNullableStatment.executeUpdate();
			}
			finally
			{
				if(updateNullableStatment != null)
				{
					try
					{
						updateNullableStatment.close();
					}
					catch(Exception e){}
				}
			}
		}
		
		if
		(
			(columnProperties.get("INVALID_SIZE") != null) ||
			(columnProperties.get("INVALID_TYPE") != null)
		)
		{
			PreparedStatement createColumnStatement = null;
			try
			{
				StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " ALTER " + columnPart + " TYPE ");
				
				IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
				
				if(columnType == null)
				{
					throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
				}
				
				sqlBuilder.append(" " + columnType.getTypeExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
				
				createColumnStatement = connection.prepareStatement(sqlBuilder.toString());
				createColumnStatement.executeUpdate();
			}
			finally
			{
				if(createColumnStatement != null)
				{
					try
					{
						createColumnStatement.close();
					}
					catch(Exception e){}
				}
			}
		}
		
		if
		(
			(columnProperties.get("INVALID_DEFAULT") != null)
		)
		{
			PreparedStatement createColumnStatement = null;
			try
			{
				if(columnSpec.getDefaultValue() == null)
				{
					createColumnStatement = connection.prepareStatement("ALTER TABLE  " + tablePart + " ALTER " + columnPart + " DROP DEFAULT ");
					createColumnStatement.executeUpdate();
				}
				else
				{
					StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " ALTER " + columnPart + " SET  ");
					
					IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
					
					if(columnType == null)
					{
						throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
					}
					
					sqlBuilder.append(" " + columnType.getDefaultValueExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
					
					createColumnStatement = connection.prepareStatement(sqlBuilder.toString());
					createColumnStatement.executeUpdate();
				}
			}
			finally
			{
				if(createColumnStatement != null)
				{
					try
					{
						createColumnStatement.close();
					}
					catch(Exception e){}
				}
			}
		}
	}
	
	public String getFunctionExpression(String function)
	{
		return function ;
	}

	@Override
	public String objectNameGuidelineFormat(SchemaSpec schemaSpec, Connection connection, String name, String type)
	{
		return name == null ? name : name.toLowerCase();
	}
	
	@Override
	public Blob createBlob(Connection connection) throws SQLException
	{
		org.postgresql.core.BaseConnection nativeConnection = connection.unwrap(org.postgresql.core.BaseConnection.class);
		return new LargeObjectBlob(nativeConnection);
	}

	@Override
	public Blob getBlob(Connection connection, ResultSet resultSet, int columnIndex) throws SQLException
	{
		long oid = resultSet.getLong(columnIndex);
		if(resultSet.wasNull())
		{
			return null;
		}
		org.postgresql.core.BaseConnection nativeConnection = connection.unwrap(org.postgresql.core.BaseConnection.class);
		return new LargeObjectBlob(nativeConnection,oid);
	}

	@Override
	public Blob getBlob(Connection connection, ResultSet resultSet, String columnLabel) throws SQLException
	{
		long oid = resultSet.getLong(columnLabel);
		if(resultSet.wasNull())
		{
			return null;
		}
		org.postgresql.core.BaseConnection nativeConnection = connection.unwrap(org.postgresql.core.BaseConnection.class);
		return new LargeObjectBlob(nativeConnection,oid);
	}
	
		@Override
	public void setBlob(Connection connection, PreparedStatement preparedStatement, Blob blob, int parameterIndex) throws SQLException
	{
		if(blob == null)
		{
			preparedStatement.setNull(parameterIndex, Types.BIGINT);
			return;
		}
		
		LargeObjectBlob loBlob = (LargeObjectBlob)blob;
		if(! loBlob.isWritable())
		{
			
			org.postgresql.core.BaseConnection nativeConnection = connection.unwrap(org.postgresql.core.BaseConnection.class);
			LargeObjectManager lobj = nativeConnection.getLargeObjectAPI();
			LargeObject largeObject = lobj.open(loBlob.getOID(), LargeObjectManager.READ);
			LargeObjectBlob deepCopy = new LargeObjectBlob(nativeConnection);
			OutputStream os = null;
			InputStream is = null;
			
			try
			{
				is = largeObject.getInputStream();
				os = deepCopy.setBinaryStream(1);
				
				byte[] buffer = new byte[1080];
				int len;
				
				while((len = is.read(buffer)) > 0)
				{
					os.write(buffer, 0, len);
				}
				
				os.flush();
				loBlob = deepCopy;
			}
			catch (Exception e) 
			{
				if(e instanceof SQLException)
				{
					throw (SQLException)e;
				}
				throw new SQLException(e.getMessage(), e);
			}
			finally
			{
				try {is.close();}catch (Exception e) {}
				try {os.close();}catch (Exception e) {}
				try {deepCopy.free();}catch (Exception e) {}
				try {largeObject.close();}catch (Exception e) {}
			}
		}
		long oid = loBlob.getOID();
		preparedStatement.setLong(parameterIndex, oid);
	}
		
	@Override
	public boolean requireCleanBlob(Connection connection)
	{
		return true;
	}

	@Override
	public void cleanBlob(Connection connection, Blob blob) throws SQLException
	{
		if(blob == null)
		{
			return;
		}
		
		LargeObjectBlob loBlob = (LargeObjectBlob)blob;
		
		org.postgresql.core.BaseConnection nativeConnection = connection.unwrap(org.postgresql.core.BaseConnection.class);
		LargeObjectManager lobj = nativeConnection.getLargeObjectAPI();
		lobj.delete(loBlob.getOID());
	}
}
