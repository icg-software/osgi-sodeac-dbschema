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
package org.sodeac.dbschema.driver.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.driver.base.DefaultDatabaseSchemaDriver;

// https://books.google.de/books?id=r13SMVABpx4C&printsec=frontcover&hl=de#v=onepage&q&f=false
//https://docs.oracle.com/database/121/DRDAS/data_type.htm#DRDAS277

@Component(service=IDatabaseSchemaDriver.class)
public class OracleDatabaseSchemaProvider extends DefaultDatabaseSchemaDriver implements IDatabaseSchemaDriver
{
	@Override
	public int handle(Connection connection) throws SQLException
	{
		if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
		{
			return IDatabaseSchemaDriver.HANDLE_DEFAULT;
		}
		return IDatabaseSchemaDriver.HANDLE_NONE;
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
		
		if
		(
			(columnProperties.get("INVALID_NULLABLE") != null) || 
			(columnProperties.get("INVALID_SIZE") != null) ||
			(columnProperties.get("INVALID_DEFAULT") != null) ||
			(columnProperties.get("INVALID_TYPE") != null)
		)
		{
			PreparedStatement createColumnStatement = null;
			try
			{
				StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " MODIFY " + columnPart + " ");
				
				IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
				
				if(columnType == null)
				{
					throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
				}
				
				sqlBuilder.append(" " + columnType.getTypeExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
				sqlBuilder.append(" " + columnType.getDefaultValueExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
				if(columnProperties.get("INVALID_NULLABLE") != null)
				{
					sqlBuilder.append(" " + (columnSpec.getNullable() ? "" : "NOT " ) + "NULL");
				}
				
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
			
			if
			(
				(columnProperties.get("INVALID_DEFAULT") != null) &&
				(columnSpec.getDefaultValue() == null)
			)
			{
				try
				{
					StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " MODIFY " + columnPart + " ");
					
					IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
					
					if(columnType == null)
					{
						throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
					}
					
					sqlBuilder.append(" " + columnType.getTypeExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
					sqlBuilder.append(" DEFAULT NULL ");
					
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
		}
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
		if("VARCHAR2".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.VARCHAR.toString();
		}
		if("LONG RAW".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BINARY.toString();
		}
		
		if((columnSpec != null) && (columnSpec.getColumntype() != null) && (columnProperties.get("COLUMN_COLUMN_SIZE") != null))
		{
			String colType = (String)columnProperties.get("COLUMN_TYPE_NAME");
			int colSize = (int)columnProperties.get("COLUMN_COLUMN_SIZE");
			
			if(IColumnType.ColumnType.BOOLEAN.toString().equals(columnSpec.getColumntype()))
			{
				if("CHAR".equalsIgnoreCase(colType) && (colSize == 1))
				{
					return IColumnType.ColumnType.BOOLEAN.toString();
				}
			}
			
			if("NUMBER".equalsIgnoreCase(colType))
			{
				if((colSize >= 5) && (colSize  < 10))
				{
					return IColumnType.ColumnType.SMALLINT.toString();
				}
				if((colSize >= 10) && (colSize  < 19))
				{
					return IColumnType.ColumnType.INTEGER.toString();
				}
				if((colSize >= 19))
				{
					return IColumnType.ColumnType.BIGINT.toString();
				}
				return null;
			}
			
			if("FLOAT".equalsIgnoreCase(colType))
			{
				if((colSize >= 63) && (colSize  < 126))
				{
					return IColumnType.ColumnType.REAL.toString();
				}
				if((colSize >= 126))
				{
					return IColumnType.ColumnType.DOUBLE.toString();
				}
				return null;
			}
			
			if(IColumnType.ColumnType.TIME.toString().equals(columnSpec.getColumntype()))
			{
				if("DATE".equalsIgnoreCase(colType) && (colSize == 7))
				{
					return IColumnType.ColumnType.TIME.toString();
				}
			}
			
			if(IColumnType.ColumnType.DATE.toString().equals(columnSpec.getColumntype()))
			{
				if("DATE".equalsIgnoreCase(colType) && (colSize == 7))
				{
					return IColumnType.ColumnType.DATE.toString();
				}
			}
			
			if("TIMESTAMP(6)".equals(colType) && (colSize == 11))
			{
				return IColumnType.ColumnType.TIMESTAMP.toString();
			}
		}
		return super.determineColumnType(connection, schemaSpec, tableSpec, columnSpec, columnProperties);
	}
	
	@Override
	public boolean tableRequiresColumn()
	{
		return true;
	}
	
	@Override
	public String catalogSearchPattern(SchemaSpec schemaSpec, Connection connection, String catalog)
	{
		return null;
	}
	
	@Override
	public String objectNameGuidelineFormat(SchemaSpec schemaSpec, Connection connection, String name, String type)
	{
		return name == null ? name : name.toUpperCase();
	}
	
	@Override
	public String getFunctionExpression(String function)
	{
		if(function.equalsIgnoreCase(IDatabaseSchemaDriver.Function.CURRENT_TIME.toString()))
		{
			return IDatabaseSchemaDriver.Function.CURRENT_DATE.toString();
		}
		return function ;
	}
	
	@Override
	public void createSchema(Connection connection, String schemaName, Map<String,Object> properties) throws SQLException
	{
		String password = schemaName;
		String permissions = "WITH ADMIN OPTION";
		
		if(properties != null)
		{
			password = properties.containsKey("ORA_SCHEMAUSER_PASSWORD") ? (String)properties.get("ORA_SCHEMAUSER_PASSWORD") : password;
			permissions = properties.containsKey("ORA_SCHEMAUSER_PERMISSIONS") ? (String)properties.get("ORA_SCHEMAUSER_PERMISSIONS") : permissions;
		}
		
		PreparedStatement prepStat = connection.prepareStatement("CREATE USER " + schemaName.toUpperCase() + " IDENTIFIED BY " + password + " DEFAULT TABLESPACE USERS PROFILE DEFAULT");
		prepStat.executeUpdate();
		prepStat.close();
		
		prepStat = connection.prepareStatement("GRANT CONNECT TO " + schemaName.toUpperCase() + " " + permissions);
		prepStat.executeUpdate();
		prepStat.close();
		
		prepStat = connection.prepareStatement("GRANT RESOURCE TO " + schemaName.toUpperCase() + " " + permissions);
		prepStat.executeUpdate();
		prepStat.close();
		
		prepStat = connection.prepareStatement("GRANT DBA TO " + schemaName.toUpperCase() + " " + permissions);
		prepStat.executeUpdate();
		prepStat.close();
	}
	
	public void dropSchema(Connection connection, String schemaName, Map<String,Object> properties) throws SQLException
	{
		if(! super.confirmDropSchema(connection, schemaName, properties))
		{
			throw new SQLException("you should confirm drop schema");
		}
		
		PreparedStatement prepStat = connection.prepareStatement("DROP USER " + objectNameGuidelineFormat(null, connection, schemaName, "SCHEMA") + " CASCADE ");
		prepStat.executeUpdate();
		prepStat.close();
		
	}
}
