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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.osgi.service.component.annotations.Component;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@Component(name="DefaultColumnType", service=IColumnType.class)
public class DefaultColumnTypeImpl implements IColumnType
{
	private static final List<String> typeList = Arrays.asList(new String[] 
	{
		ColumnType.CHAR.toString(),
		ColumnType.VARCHAR.toString(),
		ColumnType.CLOB.toString(),
		ColumnType.BOOLEAN.toString(),
		ColumnType.SMALLINT.toString(),
		ColumnType.INTEGER.toString(),
		ColumnType.BIGINT.toString(),
		ColumnType.REAL.toString(),
		ColumnType.DOUBLE.toString(),
		ColumnType.TIMESTAMP.toString(),
		ColumnType.DATE.toString(),
		ColumnType.TIME.toString(),
		ColumnType.BINARY.toString(),
		ColumnType.BLOB.toString()
	});
	
	@Override
	public List<String> getTypeList()
	{
		return typeList;
	}
	

	@Override
	public String getTypeExpression
	(
		Connection connection, 
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		ColumnSpec columnSpec, 
		String dbProduct,
		IDatabaseSchemaDriver schemaDriver
	) throws SQLException
	{
		if((columnSpec.getColumntype() == null) || ColumnType.VARCHAR.toString().equalsIgnoreCase(columnSpec.getColumntype()) || ColumnType.CHAR.toString().equalsIgnoreCase(columnSpec.getColumntype()) )
		{
			String type = (columnSpec.getColumntype() == null) || ColumnType.VARCHAR.toString().equalsIgnoreCase(columnSpec.getColumntype()) ? "VARCHAR" : "CHAR";
			
			if(columnSpec.getSize() > 0)
			{
				if(ColumnType.VARCHAR.toString().equalsIgnoreCase(type) && connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
				{
					return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, type + "(" + columnSpec.getSize() + " CHAR)", "COLUMN_TYPE") ;
				}
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, type + "(" + columnSpec.getSize() + ")", "COLUMN_TYPE") ;
			}
			else
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, type , "COLUMN_TYPE") ;
			}
		}
		
		if(ColumnType.CLOB.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "text", "COLUMN_TYPE");
			}
		}
		if(ColumnType.REAL.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "float4", "COLUMN_TYPE");
			}
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "FLOAT(63)", "COLUMN_TYPE");
			}
		}
		if(ColumnType.DOUBLE.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "float8", "COLUMN_TYPE");
			}
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "FLOAT(126)", "COLUMN_TYPE");
			}
		}
		if(ColumnType.BINARY.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "bytea", "COLUMN_TYPE");
			}
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "LONG RAW", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.BLOB.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("PostgreSQL"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "oid", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.BOOLEAN.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "CHAR(1)", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.SMALLINT.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "NUMBER(5)", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.INTEGER.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "NUMBER(10)", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.BIGINT.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "NUMBER(19)", "COLUMN_TYPE");
			}
		}
		
		if(ColumnType.TIME.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if(connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("Oracle"))
			{
				return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "DATE", "COLUMN_TYPE");
			}
		}
		
		return schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getColumntype(), "COLUMN_TYPE");
	}

	@Override
	public String getDefaultValueExpression
	(
		Connection connection, 
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		ColumnSpec columnSpec, 
		String dbProduct,
		IDatabaseSchemaDriver schemaDriver
	) throws SQLException
	{
		if((columnSpec.getColumntype() == null) || ColumnType.VARCHAR.toString().equalsIgnoreCase(columnSpec.getColumntype()) || ColumnType.CHAR.toString().equalsIgnoreCase(columnSpec.getColumntype()) )
		{
			if(columnSpec.getDefaultValue() != null)
			{
				return "DEFAULT " + (columnSpec.getDefaultValueByFunction() ? schemaDriver.getFunctionExpression(columnSpec.getDefaultValue()) :  columnSpec.getDefaultValue() );
			}
		}
		if(ColumnType.BOOLEAN.toString().equalsIgnoreCase(columnSpec.getColumntype()))
		{
			if((columnSpec.getDefaultValue() != null) && (!columnSpec.getDefaultValue().isEmpty()))
			{
				return "DEFAULT " + 
					(
						columnSpec.getDefaultValue().equalsIgnoreCase("true") ? 
						schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "TRUE" , "BOOLEAN") : 
						schemaDriver.objectNameGuidelineFormat(schemaSpec, connection, "FALSE" , "BOOLEAN")
					);
			}
		}
		
		if(ColumnType.TIMESTAMP.toString().equals(columnSpec.getColumntype()))
		{
			if((columnSpec.getDefaultValue() != null) && (!columnSpec.getDefaultValue().isEmpty()))
			{
				if(columnSpec.getDefaultValue().equals("NOW"))
				{
					return "DEFAULT " + schemaDriver.getFunctionExpression(IDatabaseSchemaDriver.Function.CURRENT_TIMESTAMP.toString()); 
				}
			}
		}
		else if(ColumnType.DATE.toString().equals(columnSpec.getColumntype()))
		{
			if((columnSpec.getDefaultValue() != null) && (!columnSpec.getDefaultValue().isEmpty()))
			{
				if(columnSpec.getDefaultValue().equals("NOW"))
				{
					return "DEFAULT " + schemaDriver.getFunctionExpression(IDatabaseSchemaDriver.Function.CURRENT_DATE.toString()); 
				}
			}
		}
		else if(ColumnType.TIME.toString().equals(columnSpec.getColumntype()))
		{
			if((columnSpec.getDefaultValue() != null) && (!columnSpec.getDefaultValue().isEmpty()))
			{
				if(columnSpec.getDefaultValue().equals("NOW"))
				{
					return "DEFAULT " + schemaDriver.getFunctionExpression(IDatabaseSchemaDriver.Function.CURRENT_TIME.toString()); 
				}
			}	
		}
		if((columnSpec.getDefaultValue() != null) && (columnSpec.getDefaultValue().length() > 0))
		{
			
			return "DEFAULT " + (columnSpec.getDefaultValueByFunction() ? schemaDriver.getFunctionExpression(columnSpec.getDefaultValue()) : columnSpec.getDefaultValue());
		}
		
		return new String();
	}
	

}
