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
package org.sodeac.dbschema.driver.base;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.osgi.service.component.annotations.Component;
import org.sodeac.dbschema.api.IndexSpec;
import org.sodeac.dbschema.api.ColumnSpec;
import org.sodeac.dbschema.api.DatabaseCommonElements;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.PrimaryKeySpec;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;
import org.sodeac.dbschema.api.IColumnType.Applicability;

@Component(service=IDatabaseSchemaDriver.class)
public class DefaultDatabaseSchemaDriver implements IDatabaseSchemaDriver
{
	protected List<IColumnType> columnDriverList = null;

	@Override
	public int handle(Connection connection) throws SQLException
	{		
		return HANDLE_FALLBACK;
	}
	
	@Override
	public String getType(Connection connection) throws SQLException
	{
		if(connection == null)
		{
			return null;
		}
		return connection.getMetaData().getDatabaseProductName();
	}
	
	public void  setColumnDriverList(List<IColumnType> columnDriverList)
	{
		this.columnDriverList = columnDriverList;
	}

	@Override
	public boolean tableExists
	(
		Connection connection, 
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		Map<String, Object> properties
	) 
	throws SQLException
	{
		String catalog = connection.getCatalog();

		String schema = connection.getSchema();
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
		}
		boolean quoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			quoted = tableSpec.getQuotedName().booleanValue();
		}
		
		String cat = null;
		String schem = null;
		String tbl;
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getTables
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), quoted, "TABLE"),
				new String[]{"TABLE"}
			);
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						in H2 the name of DB
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 						
				 * TABLE_TYPE 						"TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"
				 * REMARKS 							explanatory comment on the table 
				 * TYPE_CAT 						
				 * TYPE_SCHEM 
				 * TYPE_NAME 
				 * SELF_REFERENCING_COL_NAME 		name of the designated "identifier" column of a typed table
				 * REF_GENERATION 					specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED"
				 */
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				if(quoted && tbl.equals(tableSpec.getName()))
				{
					return true;
				}
				
				if((! quoted) && tbl.equalsIgnoreCase(tableSpec.getName()))
				{
					return true;
				}
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		try
		{
			// Try again with wildcard tablename
			
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getTables(null, null, "%", new String[]{"TABLE"});
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						in H2 the name of DB
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 						
				 * TABLE_TYPE 						"TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"
				 * REMARKS 							explanatory comment on the table 
				 * TYPE_CAT 						
				 * TYPE_SCHEM 
				 * TYPE_NAME 
				 * SELF_REFERENCING_COL_NAME 		name of the designated "identifier" column of a typed table
				 * REF_GENERATION 					specifies how values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED"
				 */
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				if(quoted && tbl.equals(tableSpec.getName()))
				{
					return true;
				}
				
				if((! quoted) && tbl.equalsIgnoreCase(tableSpec.getName()))
				{
					return true;
				}
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		return false;
	}

	@Override
	public void createTable
	(
		Connection connection,
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		Map<String, Object> properties
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
		
		boolean quoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			quoted = tableSpec.getQuotedName().booleanValue();
		}
		
		String defaultColumn = "";
		
		if(tableRequiresColumn())
		{
			defaultColumn = " " + objectNameGuidelineFormat(schemaSpec, connection, IDatabaseSchemaDriver.REQUIRED_DEFAULT_COLUMN, "COLUMN") + " char(1) NULL ";
		}
		
		String tableSpace = objectNameGuidelineFormat(schemaSpec, connection, schemaSpec.getTableSpaceData(), "TABLESPACE");
		
		if((tableSpec.getTableSpace() != null) && (!tableSpec.getTableSpace().isEmpty()))
		{
			tableSpace = objectNameGuidelineFormat(schemaSpec, connection,  tableSpec.getTableSpace(), "TABLESPACE");
		}
		
		String tableSpaceDefinition = "";
		
		if((tableSpace != null) && (! tableSpace.isEmpty()))
		{
			tableSpaceDefinition = tableSpaceAppendix(connection, schemaSpec, tableSpec, properties, tableSpace, "TABLE");
		}
		
		PreparedStatement createTableStatement = null;
		try
		{
			String sql = 
						quoted ? 
							"CREATE TABLE " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  "(" + defaultColumn + ")" + tableSpaceDefinition:
							"CREATE TABLE " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + "(" + defaultColumn + ")" + tableSpaceDefinition
						;
			createTableStatement = connection.prepareStatement(sql);
			createTableStatement.executeUpdate();
		}
		finally
		{
			if(createTableStatement != null)
			{
				try
				{
					createTableStatement.close();
				}
				catch(Exception e){}
			}
		}
	}
	
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
		return "";
	}
	
	@Override
	public boolean primaryKeyExists
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		Map<String, Object> tableProperties
	) throws SQLException
	{
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
		}
		  
		boolean tableQuoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			tableQuoted = tableSpec.getQuotedName().booleanValue();
		}
		
		ColumnSpec columnSpec = null;
		for(ColumnSpec column : tableSpec.getColumnList())
		{
			if(column.getPrimaryKey() == null)
			{
				continue;
			}
			if(columnSpec != null)
			{
				throw new RuntimeException("Multible PKs not supported !!!");
			}
			columnSpec = column;
		}
		
		if(columnSpec == null)
		{
			return true;
		}
		
		boolean columnQuoted = false;
		if(columnSpec.getQuotedName() != null)
		{
			columnQuoted = columnSpec.getQuotedName().booleanValue();
		}
		
		String cat = null;
		String schem = null;
		String tbl = null;
		String col = null;
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getPrimaryKeys
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), tableQuoted, "TABLE")
			);
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						UI / MASTER / POS .... (in H2 the name of DB )
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 		
				 * COLUMN_NAME
				 * KEY_SEQ
				 * PK_NAME							CONSTRAINT-NAME
				 */
				
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				col 	= resultSet.getString("COLUMN_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(col == null) {col = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				if(tableQuoted && (!tbl.equals(tableSpec.getName())))
				{
					continue;
				}
				if((!tableQuoted) && (!tbl.equalsIgnoreCase(tableSpec.getName())))
				{
					continue;
				}
				
				if(columnQuoted && (!col.equals(columnSpec.getName())))
				{
					continue;
				}
				if((!columnQuoted) && (!col.equalsIgnoreCase(columnSpec.getName())))
				{
					continue;
				}
				
				// Key name ignored
				
				return true;
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		return false;
	}

	@Override
	public void setPrimaryKey
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		Map<String, Object> tableProperties
	) throws SQLException
	{
		setPrimaryKeyWithoutIndex(connection, schemaSpec, tableSpec, tableProperties);
	}
	
	protected void setPrimaryKeyWithIndex
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		Map<String, Object> tableProperties
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
		
		ColumnSpec columnSpec = null;
		for(ColumnSpec column : tableSpec.getColumnList())
		{
			if(column.getPrimaryKey() == null)
			{
				continue;
			}
			if(columnSpec != null)
			{
				throw new RuntimeException("Multible PKs not supported !!! ... Not Yet");
			}
			columnSpec = column;
		}
		
		boolean columnQuoted = false;
		if(columnSpec.getQuotedName() != null)
		{
			columnQuoted = columnSpec.getQuotedName().booleanValue();
		}
		
		PrimaryKeySpec primaryKey = columnSpec.getPrimaryKey();
		
		if(primaryKey == null)
		{
			return;
		}
		
		boolean nameQuoted = false;
		
		if(columnSpec.getQuotedName() != null)
		{
			nameQuoted = primaryKey.getQuotedName().booleanValue();
		}
		
		String indexName = primaryKey.getIndexName();
		if((indexName == null) || indexName.isEmpty())
		{
			indexName = "PKX_" + tableSpec.getName().toUpperCase();
		}
		
		if(nameQuoted)
		{
			indexName = quotedChar() + indexName + quotedChar();
		}
		else
		{
			indexName = objectNameGuidelineFormat(schemaSpec, connection, indexName, "INDEX");
		}
		
		String constraintName = primaryKey.getConstraintName();
		if((constraintName == null) || constraintName.isEmpty())
		{
			constraintName = "PK_" + tableSpec.getName().toUpperCase();
		}
		
		if(nameQuoted)
		{
			constraintName = quotedChar() + constraintName + quotedChar();
		}
		else
		{
			constraintName = objectNameGuidelineFormat(schemaSpec, connection, constraintName, "CONSTRAINT");
		}
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String columnPart =  columnQuoted ? 
				" " + quotedChar() +  "" + columnSpec.getName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getName(), "COLUMN") + " " ;
		
		String tableSpace = objectNameGuidelineFormat(schemaSpec, connection, schemaSpec.getTableSpaceIndex(), "TABLESPACE");
		
		if((columnSpec.getPrimaryKey().getTableSpace() != null) && (!columnSpec.getPrimaryKey().getTableSpace().isEmpty()))
		{
			tableSpace = objectNameGuidelineFormat(schemaSpec, connection,  columnSpec.getPrimaryKey().getTableSpace(), "TABLESPACE");
		}
		String tableSpaceDefinition = "";
		
		if((tableSpace != null) && (! tableSpace.isEmpty()))
		{
			tableSpaceDefinition = tableSpaceAppendix(connection, schemaSpec, tableSpec, tableProperties, tableSpace, "PRIMARYKEY");
		}
		
		PreparedStatement createPKStatement = null;
		try
		{
			String createPK = "ALTER TABLE " + tablePart + " ADD CONSTRAINT " + constraintName  + " PRIMARY KEY(" + columnPart + ") INDEX " + indexName + " " + tableSpaceDefinition;
			createPKStatement = connection.prepareStatement(createPK);
			createPKStatement.executeUpdate();			
		}
		finally
		{
			if(createPKStatement != null)
			{
				try
				{
					createPKStatement.close();
				}
				catch(Exception e){}
			}
		}
	}
	
	protected void setPrimaryKeyWithoutIndex
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		Map<String, Object> tableProperties
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
		
		ColumnSpec columnSpec = null;
		for(ColumnSpec column : tableSpec.getColumnList())
		{
			if(column.getPrimaryKey() == null)
			{
				continue;
			}
			if(columnSpec != null)
			{
				throw new RuntimeException("Multible PKs not supported !!! ... Not Yet");
			}
			columnSpec = column;
		}
		
		boolean columnQuoted = false;
		if(columnSpec.getQuotedName() != null)
		{
			columnQuoted = columnSpec.getQuotedName().booleanValue();
		}
		
		PrimaryKeySpec primaryKey = columnSpec.getPrimaryKey();
		
		if(primaryKey == null)
		{
			return;
		}
		
		boolean nameQuoted = false;
		
		if(columnSpec.getQuotedName() != null)
		{
			nameQuoted = primaryKey.getQuotedName().booleanValue();
		}
		
		String constraintName = primaryKey.getConstraintName();
		if((constraintName == null) || constraintName.isEmpty())
		{
			constraintName = "PK_" + tableSpec.getName().toUpperCase();
		}
		
		if(nameQuoted)
		{
			constraintName = quotedChar() + constraintName + quotedChar();
		}
		else
		{
			constraintName = objectNameGuidelineFormat(schemaSpec, connection, constraintName, "CONSTRAINT");
		}
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String columnPart =  columnQuoted ? 
				" " + quotedChar() +  "" + columnSpec.getName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getName(), "COLUMN") + " " ;
		
		String tableSpace = objectNameGuidelineFormat(schemaSpec, connection, schemaSpec.getTableSpaceIndex(), "TABLESPACE");
		
		if((columnSpec.getPrimaryKey().getTableSpace() != null) && (!columnSpec.getPrimaryKey().getTableSpace().isEmpty()))
		{
			tableSpace = objectNameGuidelineFormat(schemaSpec, connection,  columnSpec.getPrimaryKey().getTableSpace(), "TABLESPACE");
		}
		String tableSpaceDefinition = "";
		
		if((tableSpace != null) && (! tableSpace.isEmpty()))
		{
			tableSpaceDefinition = tableSpaceAppendix(connection, schemaSpec, tableSpec, tableProperties, tableSpace, "PRIMARYKEY");
		}
		
		
		PreparedStatement createPKStatement = null;
		try
		{
			String createPK = "ALTER TABLE " + tablePart + " ADD CONSTRAINT " + constraintName  + " PRIMARY KEY(" + columnPart + ") " + tableSpaceDefinition;
			createPKStatement = connection.prepareStatement(createPK);
			createPKStatement.executeUpdate();			
		}
		finally
		{
			if(createPKStatement != null)
			{
				try
				{
					createPKStatement.close();
				}
				catch(Exception e){}
			}
		}
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
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
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
		
		String cat = null;
		String schem = null;
		String tbl = null;
		String col = null;
		
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getColumns
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), tableQuoted, "TABLE"),
				objectSearchPattern(schemaSpec, connection, columnSpec.getName(), columnQuoted, "COLUMN")
			);
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						in H2 the name of DB
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 						
				 * COLUMN_NAME 						column name
				 * DATA_TYPE 						SQL type from java.sql.Types (int)
				 * TYPE_NAME						SQL type - Name
				 * COLUMN_SIZE						size (int)
				 * DECIMAL_DIGITS					digits for float .... (int)
				 * NULLABLE 						nullable

				 */
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				col 	= resultSet.getString("COLUMN_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(col == null) {col = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				if(tableQuoted && (!tbl.equals(tableSpec.getName())))
				{
					continue;
				}
				if((!tableQuoted) && (!tbl.equalsIgnoreCase(tableSpec.getName())))
				{
					continue;
				}
				
				if(columnQuoted && (!col.equals(columnSpec.getName())))
				{
					continue;
				}
				if((!columnQuoted) && (!col.equalsIgnoreCase(columnSpec.getName())))
				{
					continue;
				}
							
				properties.put("COLUMN_TABLE_CAT", cat);
				properties.put("COLUMN_TABLE_SCHEM", schem);
				properties.put("COLUMN_TABLE_NAME", tbl);
				
				properties.put("COLUMN_COLUMN_NAME", col);
				properties.put("COLUMN_DATA_TYPE", resultSet.getInt("DATA_TYPE"));
				properties.put("COLUMN_TYPE_NAME", resultSet.getString("TYPE_NAME"));
				properties.put("COLUMN_COLUMN_SIZE", resultSet.getInt("COLUMN_SIZE"));
				properties.put("COLUMN_DECIMAL_DIGITS", resultSet.getInt("DECIMAL_DIGITS"));
				properties.put("COLUMN_NULLABLE", resultSet.getInt("NULLABLE"));
				properties.put("COLUMN_COLUMN_DEF",resultSet.getString("COLUMN_DEF"));
				
				return true;
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getColumns(null,null,"%","%");
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						in H2 the name of DB
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 						
				 * COLUMN_NAME 						column name
				 * DATA_TYPE 						SQL type from java.sql.Types (int)
				 * TYPE_NAME						SQL type - Name
				 * COLUMN_SIZE						size (int)
				 * DECIMAL_DIGITS					digits for float .... (int)
				 * NULLABLE 						nullable

				 */
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				col 	= resultSet.getString("COLUMN_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(col == null) {col = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				if(tableQuoted && (!tbl.equals(tableSpec.getName())))
				{
					continue;
				}
				if((!tableQuoted) && (!tbl.equalsIgnoreCase(tableSpec.getName())))
				{
					continue;
				}
				
				if(columnQuoted && (!col.equals(columnSpec.getName())))
				{
					continue;
				}
				if((!columnQuoted) && (!col.equalsIgnoreCase(columnSpec.getName())))
				{
					continue;
				}
				
				properties.put("COLUMN_TABLE_CAT", cat);
				properties.put("COLUMN_TABLE_SCHEM", schem);
				properties.put("COLUMN_TABLE_NAME", tbl);
				
				properties.put("COLUMN_COLUMN_NAME", col);
				properties.put("COLUMN_DATA_TYPE", resultSet.getInt("DATA_TYPE"));
				properties.put("COLUMN_TYPE_NAME", resultSet.getString("TYPE_NAME"));
				properties.put("COLUMN_COLUMN_SIZE", resultSet.getInt("COLUMN_SIZE"));
				properties.put("COLUMN_DECIMAL_DIGITS", resultSet.getInt("DECIMAL_DIGITS"));
				properties.put("COLUMN_NULLABLE", resultSet.getInt("NULLABLE"));
				properties.put("COLUMN_COLUMN_DEF",resultSet.getString("COLUMN_DEF"));
				
				return true;
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		return false;
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
		for(IColumnType.ColumnType type : IColumnType.ColumnType.values())
		{
			if(type.toString().equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
			{
				return type.toString();
			}
		}
		if("bool".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BOOLEAN.toString();
		}
		if("text".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.CLOB.toString();
		}
		if("int2".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.SMALLINT.toString();
		}
		if("int4".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.INTEGER.toString();
		}
		if("int8".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BIGINT.toString();
		}
		if("float4".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.REAL.toString();
		}
		if("float8".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.DOUBLE.toString();
		}
		if("varbinary".equalsIgnoreCase(columnProperties.get("COLUMN_TYPE_NAME").toString()))
		{
			return IColumnType.ColumnType.BINARY.toString();
		}
		return null;
	}

	@Override
	public void createColumn
	(
		Connection connection,
		SchemaSpec schemaSpec, 
		TableSpec tableSpec,
		ColumnSpec columnSpec, 
		Map<String, Object> properties
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
				
		PreparedStatement createColumnStatement = null;
		try
		{
			StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " ADD " + columnPart + " ");
			
			IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
			
			if(columnType == null)
			{
				throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
			}
			
			sqlBuilder.append(" " + columnType.getTypeExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
			sqlBuilder.append(" " + columnType.getDefaultValueExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
			
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
	
	public void dropColumn(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, String columnName, boolean quoted)throws SQLException
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
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String columnPart =  quoted ? 
				" " + quotedChar() +  "" + columnName + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnName, "COLUMN") + " " ;
				
		PreparedStatement createColumnStatement = null;
		try
		{
			StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " DROP COLUMN " + columnPart + " ");
			
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

	@Override
	public boolean isValidColumnProperties
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		ColumnSpec columnSpec, Map<String, Object> columnProperties
	) throws SQLException
	{
		
		boolean valid = true;
		
		// Nullable
		
		if(columnProperties.get("COLUMN_NULLABLE") != null) // exists
		{
			boolean nullable = ((Integer)columnProperties.get("COLUMN_NULLABLE")).intValue() > 0;
			if(nullable != columnSpec.getNullable())
			{
				valid = false;
				columnProperties.put("INVALID_NULLABLE", true);
			}
		}
		else // not exists (nullable is default)
		{
			if(! columnSpec.getNullable())
			{
				valid = false;
				columnProperties.put("INVALID_NULLABLE", true);
			}
		}
		
		// TYPE
		
		if(columnProperties.get("COLUMN_TYPE_NAME") != null) // exists
		{
			String type = determineColumnType(connection, schemaSpec, tableSpec, columnSpec, columnProperties);
			if(type != null)
			{
				if(! type.equalsIgnoreCase(columnSpec.getColumntype()))
				{
					valid = false;
					columnProperties.put("INVALID_TYPE", true);
				}
			}
		}
		
		if(columnProperties.containsKey("COLUMN_COLUMN_DEF")) // exists
		{
			String defaultValue = columnSpec.getDefaultValue();
			if((defaultValue == null) || defaultValue.isEmpty())
			{
				if((columnProperties.get("COLUMN_COLUMN_DEF") != null) &&  (! ((String)columnProperties.get("COLUMN_COLUMN_DEF")).isEmpty()))
				{
					if
					(! 
						(
							((String)columnProperties.get("COLUMN_COLUMN_DEF")).equalsIgnoreCase("null") ||
							((String)columnProperties.get("COLUMN_COLUMN_DEF")).equalsIgnoreCase("null ")
						)
					)
					{
						valid = false;
						columnProperties.put("INVALID_DEFAULT", true);
					}
				}
			}
			else
			{
				if(columnSpec.getDefaultValueByFunction())
				{
					defaultValue = getFunctionExpression(defaultValue);
				}
				
				if(! defaultValue.equalsIgnoreCase((String)columnProperties.get("COLUMN_COLUMN_DEF")))
				{
					valid = false;
					columnProperties.put("INVALID_DEFAULT", true);
				}
			}
		}
		
		if(columnProperties.get("COLUMN_COLUMN_SIZE") != null) // exists
		{
			if
			(
				(
					IColumnType.ColumnType.CHAR.toString().equals(columnSpec.getColumntype())
					||
					IColumnType.ColumnType.VARCHAR.toString().equals(columnSpec.getColumntype())
				)
				&&
				columnSpec.getSize() > 0
			)
			{
				if(((int)columnProperties.get("COLUMN_COLUMN_SIZE")) != columnSpec.getSize())
				{
					valid = false;
					columnProperties.put("INVALID_SIZE", true);
				}
			}
		}
		
		return valid;
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
				String nullableStatement = "ALTER TABLE  " + tablePart +  " ALTER COLUMN " + columnPart + " SET " + ( nullable ? "" : "NOT" ) + " NULL";
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
			(columnProperties.get("INVALID_DEFAULT") != null) ||
			(columnProperties.get("INVALID_TYPE") != null)
		)
		{
			PreparedStatement createColumnStatement = null;
			try
			{
				StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " ALTER " + columnPart + " ");
				
				IColumnType columnType = findBestColumnType(connection, schemaSpec, tableSpec, columnSpec);
				
				if(columnType == null)
				{
					throw new SQLException("No ColumnType Provider found for \"" + columnSpec.getColumntype()+ "\"");
				}
				
				sqlBuilder.append(" " + columnType.getTypeExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
				sqlBuilder.append(" " + columnType.getDefaultValueExpression(connection, schemaSpec, tableSpec, columnSpec, "TODO", this));
				
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
			(columnProperties.get("INVALID_DEFAULT") != null) &&
			(columnSpec.getDefaultValue() == null)
		)
		{
			PreparedStatement createColumnStatement = null;
			try
			{
				String sql =  "ALTER TABLE  " + tablePart + " ALTER " + columnPart + " DROP DEFAULT ";
				
				createColumnStatement = connection.prepareStatement(sql);
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

	@Override
	public boolean isValidForeignKey
	(
		Connection connection, SchemaSpec schemaSpec,
		TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties
	) 
	throws SQLException
	{	
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
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
		
		String cat = null;
		String schem = null;
		String tbl = null;
		String col = null;
		String keyName = null;
		
		boolean keyQuoted = false;
		if((columnSpec.getForeignKey() != null) && (columnSpec.getForeignKey().getQuotedKeyName() != null))
		{
			keyQuoted = columnSpec.getForeignKey().getQuotedKeyName().booleanValue();
		}
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getImportedKeys
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), tableQuoted, "TABLE")
			);
			while(resultSet.next())
			{
				/*
				 * PKTABLE_CAT String => primary key table catalog being imported (may be null)
				 * PKTABLE_SCHEM String => primary key table schema being imported (may be null)
				 * PKTABLE_NAME String => primary key table name being imported
				 * PKCOLUMN_NAME String => primary key column name being imported
				 * FKTABLE_CAT String => foreign key table catalog (may be null)
				 * FKTABLE_SCHEM String => foreign key table schema (may be null)
				 * FKTABLE_NAME String => foreign key table name
				 * FKCOLUMN_NAME String => foreign key column name
				 * KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
				 * UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
				 * 		importedNoAction - do not allow update of primary key if it has been imported
				 * 		importedKeyCascade - change imported key to agree with primary key update
				 * 		importedKeySetNull - change imported key to NULL if its primary key has been updated
				 * 		importedKeySetDefault - change imported key to default values if its primary key has been updated
				 * 		importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
				 * DELETE_RULE short => What happens to the foreign key when primary is deleted.
				 * 		importedKeyNoAction - do not allow delete of primary key if it has been imported
				 * 		importedKeyCascade - delete rows that import a deleted key
				 * 		importedKeySetNull - change imported key to NULL if its primary key has been deleted
				 * 		importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
				 * 		importedKeySetDefault - change imported key to default if its primary key has been deleted
				 * FK_NAME String => foreign key name (may be null)
				 * PK_NAME String => primary key name (may be null)
				 * DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
				 * 		importedKeyInitiallyDeferred - see SQL92 for definition
				 * 		importedKeyInitiallyImmediate - see SQL92 for definition
				 * 		importedKeyNotDeferrable - see SQL92 for definition
				 * 
				 * PKTABLE_CAT: 		SODEAC
				 * PKTABLE_SCHEM: 		PUBLIC
				 * PKTABLE_NAME: 		SODEAC_DOMAIN
				 * PKCOLUMN_NAME: 		ID
				 * FKTABLE_CAT: 		SODEAC
				 * FKTABLE_SCHEM: 		PUBLIC
				 * FKTABLE_NAME: 		SODEAC_USER
				 * FKCOLUMN_NAME: 		SODEAC_DOMAIN_ID
				 * KEY_SEQ: 			1
				 * UPDATE_RULE: 		1
				 * DELETE_RULE: 		1
				 * FK_NAME: 			CONSTRAINT_F
				 * PK_NAME: 			PRIMARY_KEY_4
				 * DEFERRABILITY:		7
				 */
				
				cat 	= resultSet.getString("FKTABLE_CAT");
				schem 	= resultSet.getString("FKTABLE_SCHEM");
				tbl 	= resultSet.getString("FKTABLE_NAME");
				col 	= resultSet.getString("FKCOLUMN_NAME");
				keyName = resultSet.getString("FK_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(keyName == null) {keyName = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}

				boolean tableNameMatch = false;
				if(tableQuoted && tbl.equals(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				if((!tableQuoted) && tbl.equalsIgnoreCase(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				
				boolean columnNameMatch = false;
				if(columnQuoted && col.equals(columnSpec.getName()))
				{
					columnNameMatch = true;
				}
				if((!columnQuoted) && col.equalsIgnoreCase(columnSpec.getName()))
				{
					columnNameMatch = true;
				}
				
				if((columnSpec.getForeignKey() == null) && tableNameMatch && columnNameMatch)
				{
					return false;
				}
				
				if(columnSpec.getForeignKey() == null)
				{
					continue;
				}
				
				boolean keyNameMatch = false;
				if(keyQuoted && (keyName.equals(columnSpec.getForeignKey().getConstraintName() == null ? "" : columnSpec.getForeignKey().getConstraintName())))
				{
					keyNameMatch = true;
				}
				if((!keyQuoted) && (keyName.equalsIgnoreCase(columnSpec.getForeignKey().getConstraintName() == null ? "" :  columnSpec.getForeignKey().getConstraintName())))
				{
					keyNameMatch = true;
				}
				
				
				if(keyNameMatch && ((! columnNameMatch) || (! tableNameMatch)))
				{
					columnProperties.put("CLEAN_FK", true);
					return false;
				}
				
				if((! columnNameMatch) || (! tableNameMatch))
				{
					continue;
				}
				
				if(! keyNameMatch)
				{
					continue;
				}
				
				if
				(
					resultSet.getString("PKTABLE_NAME").equalsIgnoreCase(columnSpec.getForeignKey().getTableName()) &&
					resultSet.getString("PKCOLUMN_NAME").equalsIgnoreCase(columnSpec.getForeignKey().getReferencedColumnName()) 
				)
				{
					return true;
				}
				columnProperties.put("CLEAN_FK", true);
				return false;
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		return columnSpec.getForeignKey() == null;
	}
	
	protected void cleanColumnForeignKeys
	(
		Connection connection, SchemaSpec schemaSpec,
		TableSpec tableSpec, ColumnSpec columnSpec
	) 
	throws SQLException
	{	
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
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
		
		String cat = null;
		String schem = null;
		String tbl = null;
		String col = null;
		String keyName = null;
		
		List<String> toDelete = new ArrayList<>();
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getImportedKeys
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), tableQuoted, "TABLE")
			);
			while(resultSet.next())
			{
				/*
				 * PKTABLE_CAT String => primary key table catalog being imported (may be null)
				 * PKTABLE_SCHEM String => primary key table schema being imported (may be null)
				 * PKTABLE_NAME String => primary key table name being imported
				 * PKCOLUMN_NAME String => primary key column name being imported
				 * FKTABLE_CAT String => foreign key table catalog (may be null)
				 * FKTABLE_SCHEM String => foreign key table schema (may be null)
				 * FKTABLE_NAME String => foreign key table name
				 * FKCOLUMN_NAME String => foreign key column name
				 * KEY_SEQ short => sequence number within a foreign key( a value of 1 represents the first column of the foreign key, a value of 2 would represent the second column within the foreign key).
				 * UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
				 * 		importedNoAction - do not allow update of primary key if it has been imported
				 * 		importedKeyCascade - change imported key to agree with primary key update
				 * 		importedKeySetNull - change imported key to NULL if its primary key has been updated
				 * 		importedKeySetDefault - change imported key to default values if its primary key has been updated
				 * 		importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
				 * DELETE_RULE short => What happens to the foreign key when primary is deleted.
				 * 		importedKeyNoAction - do not allow delete of primary key if it has been imported
				 * 		importedKeyCascade - delete rows that import a deleted key
				 * 		importedKeySetNull - change imported key to NULL if its primary key has been deleted
				 * 		importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
				 * 		importedKeySetDefault - change imported key to default if its primary key has been deleted
				 * FK_NAME String => foreign key name (may be null)
				 * PK_NAME String => primary key name (may be null)
				 * DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
				 * 		importedKeyInitiallyDeferred - see SQL92 for definition
				 * 		importedKeyInitiallyImmediate - see SQL92 for definition
				 * 		importedKeyNotDeferrable - see SQL92 for definition
				 * 
				 * PKTABLE_CAT: 		SODEAC
				 * PKTABLE_SCHEM: 		PUBLIC
				 * PKTABLE_NAME: 		SODEAC_DOMAIN
				 * PKCOLUMN_NAME: 		ID
				 * FKTABLE_CAT: 		SODEAC
				 * FKTABLE_SCHEM: 		PUBLIC
				 * FKTABLE_NAME: 		SODEAC_USER
				 * FKCOLUMN_NAME: 		SODEAC_DOMAIN_ID
				 * KEY_SEQ: 			1
				 * UPDATE_RULE: 		1
				 * DELETE_RULE: 		1
				 * FK_NAME: 			CONSTRAINT_F
				 * PK_NAME: 			PRIMARY_KEY_4
				 * DEFERRABILITY:		7
				 */
				
				cat 	= resultSet.getString("FKTABLE_CAT");
				schem 	= resultSet.getString("FKTABLE_SCHEM");
				tbl 	= resultSet.getString("FKTABLE_NAME");
				col 	= resultSet.getString("FKCOLUMN_NAME");
				keyName = resultSet.getString("FK_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(keyName == null) {keyName = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog) || cat.equalsIgnoreCase(schema)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}

				boolean tableNameMatch = false;
				if(tableQuoted && tbl.equals(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				if((!tableQuoted) && tbl.equalsIgnoreCase(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				
				boolean columnNameMatch = false;
				if(columnQuoted && col.equals(columnSpec.getName()))
				{
					columnNameMatch = true;
				}
				if((!columnQuoted) && col.equalsIgnoreCase(columnSpec.getName()))
				{
					columnNameMatch = true;
				}
				
				if((! columnNameMatch) || (! tableNameMatch))
				{
					continue;
				}
				
				toDelete.add(keyName);
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		for(String toDeleteKey : toDelete)
		{
			dropForeignKey(connection, schemaSpec, tableSpec, toDeleteKey, true);
		}
	}
	
	public void dropForeignKey(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, String keyName, boolean quoted) throws SQLException
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
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String keyPart =  quoted ? 
				" " + quotedChar() +  "" + keyName + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, keyName, "FOREIGNKEY") + " " ;
				
		PreparedStatement createColumnStatement = null;
		try
		{
			StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE  " + tablePart + " DROP CONSTRAINT " + keyPart + " ");
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

	@Override
	public void setValidForeignKey
	(
		Connection connection, SchemaSpec schemaSpec,
		TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties
	) 
	throws SQLException
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
		
		boolean tableQuoted = tableSpec.getQuotedName() == null ? false : tableSpec.getQuotedName().booleanValue();
		boolean columnQuoted = columnSpec.getQuotedName() == null ? false : columnSpec.getQuotedName().booleanValue();
		boolean keyQuoted = ( columnSpec.getForeignKey() == null || columnSpec.getForeignKey().getQuotedKeyName() == null) ? false : columnSpec.getForeignKey().getQuotedKeyName().booleanValue();
		boolean refTableQuoted = ( columnSpec.getForeignKey() == null || columnSpec.getForeignKey().getQuotedRefTableName() == null ) ? false : columnSpec.getForeignKey().getQuotedRefTableName().booleanValue();
		boolean refColumnQuoted = ( columnSpec.getForeignKey() == null || columnSpec.getForeignKey().getQuotedRefColumnName() == null ) ? false : columnSpec.getForeignKey().getQuotedRefColumnName().booleanValue();
		
		if((columnProperties.get("CLEAN_FK") != null) && ((Boolean)columnProperties.get("CLEAN_FK")).booleanValue() && (columnSpec.getForeignKey() != null))
		{
			dropForeignKey(connection, schemaSpec, tableSpec, columnSpec.getForeignKey().getConstraintName(), keyQuoted);
		}
		
		cleanColumnForeignKeys(connection, schemaSpec, tableSpec, columnSpec);
		
		if(columnSpec.getForeignKey() == null)
		{
			return;
		}
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
			
		String columnPart =  columnQuoted ? 
				" " + quotedChar() +  "" + columnSpec.getName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getName(), "COLUMN") + " " ;
		
		String constraintPart = keyQuoted ?  
				" " + quotedChar() +  "" + columnSpec.getForeignKey().getConstraintName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getForeignKey().getConstraintName(), "FOREIGNKEY") + " " ;
		
		String refTablePart = refTableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + columnSpec.getForeignKey().getTableName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getForeignKey().getTableName(), "TABLE") + " " ;
		
		String refColumnPart = refColumnQuoted ? 
				" " + quotedChar() +  "" + columnSpec.getForeignKey().getReferencedColumnName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getForeignKey().getReferencedColumnName(), "COLUMN") + " " ;
		
		PreparedStatement createFKStatement = null;
		try
		{
			String createFK = "ALTER TABLE  " + tablePart + " ADD CONSTRAINT " + constraintPart + " FOREIGN KEY (" + columnPart + ") REFERENCES " 
					+	refTablePart + " (" + refColumnPart + ") ";
			createFKStatement = connection.prepareStatement(createFK);
			createFKStatement.executeUpdate();
		}
		finally
		{
			if(createFKStatement != null)
			{
				try
				{
					createFKStatement.close();
				}
				catch(Exception e){}
			}
		}
	}

	@Override
	public boolean isValidIndex
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		IndexSpec indexSpec, Map<String, Object> columnIndexProperties
	)
	throws SQLException
	{
		String catalog = connection.getCatalog();
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		if((tableSpec.getDbmsSchemaName() != null) && (! tableSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = tableSpec.getDbmsSchemaName();
		}
		  
		boolean tableQuoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			tableQuoted = tableSpec.getQuotedName().booleanValue();
		}
		
		boolean indexQuoted = false;
		if(indexSpec.getQuotedName() != null)
		{
			indexQuoted = indexSpec.getQuotedName().booleanValue();
		}
		
		String cat = null;
		String schem = null;
		String tbl = null;
		String col = null;
		String idx = null;
		
		ResultSet resultSet = null;
		try
		{
			
			ColumnSpec contextColumn = null;
			List<ColumnSpec> columnListOfIndex = new ArrayList<ColumnSpec>();
			for(ColumnSpec column : indexSpec.getColumns())
			{
				columnListOfIndex.add(column);
				if(column.getName().equalsIgnoreCase(DatabaseCommonElements.CONTEXT))
				{
					contextColumn = column;
				}
			}
			
			if(indexSpec.getIncludeContext() && (contextColumn == null))
			{
				contextColumn = new ColumnSpec(tableSpec,DatabaseCommonElements.CONTEXT, IColumnType.ColumnType.CHAR.toString(), false,36);
				columnListOfIndex.add(contextColumn);
			}
			
			if(columnListOfIndex.isEmpty())
			{
				return false;
			}
			
			if(indexSpec.getIndexName() == null)
			{
				return false;
			}
			
			if(indexSpec.getIndexName().isEmpty())
			{
				return false;
			}
			
			Map<String,Short> columnExists = new HashMap<String,Short>();
			boolean unique = false;
			
			
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getIndexInfo
			(
				catalogSearchPattern(schemaSpec, connection, catalog), 
				schemaSearchPattern(schemaSpec, connection, schema),  
				objectSearchPattern(schemaSpec, connection, tableSpec.getName(), tableQuoted, "TABLE"), 
				false, false
			);
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT String => table catalog (may be null)
				 * TABLE_SCHEM String => table schema (may be null)
				 * TABLE_NAME String => table name
				 * NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
				 * INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
				 * INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
				 * TYPE short => index type:
				 * 		tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
				 * 		tableIndexClustered - this is a clustered index
				 * 		tableIndexHashed - this is a hashed index
				 * 		tableIndexOther - this is some other style of index
				 * ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
				 * COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
				 * ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE is tableIndexStatistic
				 * CARDINALITY long => When TYPE is tableIndexStatistic, then this is the number of rows in the table; otherwise, it is the number of unique values in the index.
				 * PAGES long => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for the current index.
				 * FILTER_CONDITION String => Filter condition, if any. (may be null)
				 * 
				 * Key:
				 * 	TABLE_CAT: SODEAC
				 * 	TABLE_SCHEM: PUBLIC
				 * 	TABLE_NAME: SODEAC_USER
				 * 	NON_UNIQUE: false
				 * 	INDEX_QUALIFIER: SODEAC
				 * 	INDEX_NAME: IDX2
				 * 	TYPE: 3
				 * 	ORDINAL_POSITION: 1
				 * 	COLUMN_NAME: SODEAC_DOMAIN_ID
				 * 	ASC_OR_DESC: A
				 * 	CARDINALITY: 0
				 * 	PAGES : 0
				 * 	FILTER_CONDITION: 
				 * Key:
				 * 	TABLE_CAT: SODEAC
				 * 	TABLE_SCHEM: PUBLIC
				 * 	TABLE_NAME: SODEAC_USER
				 * 	NON_UNIQUE: false
				 * 	INDEX_QUALIFIER: SODEAC
				 * 	INDEX_NAME: IDX2
				 * 	TYPE: 3
				 * 	ORDINAL_POSITION: 2
				 * 	COLUMN_NAME: LOGIN_NAME
				 * 	ASC_OR_DESC: A
				 * 	CARDINALITY: 0
				 * 	PAGES : 0
				 * 	FILTER_CONDITION: 
				 */
				
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				col 	= resultSet.getString("COLUMN_NAME");
				idx		= resultSet.getString("INDEX_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(cat == null) {cat = "";}
				if(schem == null) {schem = "";}
				if(tbl == null) {tbl = "";}
				if(idx ==  null) {idx = "";}
				
				if(!(cat.isEmpty() || cat.equalsIgnoreCase("null") || cat.equalsIgnoreCase(catalog)))
				{
					continue;
				}
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				
				boolean tableNameMatch = false;
				if(tableQuoted && tbl.equals(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				if((!tableQuoted) && tbl.equalsIgnoreCase(tableSpec.getName()))
				{
					tableNameMatch = true;
				}
				
				if(! tableNameMatch)
				{
					continue;
				}
				
				boolean indexNameMatch = false;
				if(indexQuoted && (idx.equals(indexSpec.getIndexName())))
				{
					indexNameMatch = true;
				}
				if((!indexQuoted) && (idx.equalsIgnoreCase(indexSpec.getIndexName())))
				{
					indexNameMatch = true;
				}
				
				if(! indexNameMatch)
				{
					continue;
				}
				
				unique = ! resultSet.getBoolean("NON_UNIQUE");
				columnExists.put(col.toUpperCase(), resultSet.getShort("ORDINAL_POSITION"));
			}
			resultSet.close();
			
			boolean diff = false;
			boolean keyExists = ! columnExists.isEmpty();
			
			if(unique != indexSpec.getUnique())
			{
				diff = true;
			}
			else if((columnExists.size() != columnListOfIndex.size()))
			{
				diff = true;
			}
			else 
			{
				for(ColumnSpec column : columnListOfIndex)
				{
					if(!columnExists.containsKey(column.getName().toUpperCase()))
					{
						diff = true;
						break;
					}
				}
			}
			
			if(keyExists && diff)
			{
				columnIndexProperties.put("CLEAR_INDEX", true);
				
				keyExists = false;
			}
			return keyExists;
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
	}

	@Override
	public void setValidIndex
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec,
		IndexSpec indexSpec, Map<String, Object> columnIndexProperties
	)
	throws SQLException
	{
		String schema = connection.getSchema();
		
		boolean tableQuoted = false;
		if(tableSpec.getQuotedName() != null)
		{
			tableQuoted = tableSpec.getQuotedName().booleanValue();
		}
		
		boolean indexQuoted = false;
		if(indexSpec.getQuotedName() != null)
		{
			indexQuoted = indexSpec.getQuotedName().booleanValue();
		}
		
		ColumnSpec contextColumn = null;
		List<ColumnSpec> columnListOfIndex = new ArrayList<ColumnSpec>();
		for(ColumnSpec column : indexSpec.getColumns())
		{
			columnListOfIndex.add(column);
			if(column.getName().equalsIgnoreCase(DatabaseCommonElements.CONTEXT))
			{
				contextColumn = column;
			}
		}
		
		if(indexSpec.getIncludeContext() && (contextColumn == null))
		{
			contextColumn = new ColumnSpec(tableSpec,DatabaseCommonElements.CONTEXT, IColumnType.ColumnType.CHAR.toString(), false,36);
			columnListOfIndex.add(contextColumn);
		}
		
		if((columnIndexProperties.get("CLEAR_INDEX") != null) && ((Boolean)columnIndexProperties.get("CLEAR_INDEX")).booleanValue())
		{
			dropIndex(connection, schemaSpec, tableSpec, indexSpec.getIndexName(), indexSpec.getQuotedName() == null ? false : indexSpec.getQuotedName().booleanValue());
		}
		
		String tablePart = tableQuoted ? 
				" " + schema + "." + quotedChar() +  "" + tableSpec.getName() + "" + quotedChar() +  " " :
				" " + schema + "." + objectNameGuidelineFormat(schemaSpec, connection, tableSpec.getName(), "TABLE") + " " ;
		
		String indexPart =  indexQuoted ? 
				" " + quotedChar() +  "" + indexSpec.getIndexName() + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, indexSpec.getIndexName(), "INDEX") + " " ;
		
		String tableSpace = objectNameGuidelineFormat(schemaSpec, connection, schemaSpec.getTableSpaceIndex(), "TABLESPACE");
		
		if((indexSpec.getTableSpace() != null) && (!indexSpec.getTableSpace().isEmpty()))
		{
			tableSpace = objectNameGuidelineFormat(schemaSpec, connection,  indexSpec.getTableSpace(), "TABLESPACE");
		}
		String tableSpaceDefinition = "";
		
		if((tableSpace != null) && (! tableSpace.isEmpty()))
		{
			tableSpaceDefinition = tableSpaceAppendix(connection, schemaSpec, tableSpec, columnIndexProperties, tableSpace, "INDEX");
		}
	
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("CREATE ");
		if(indexSpec.getUnique())
		{
			sqlBuilder.append("UNIQUE ");
		}
		sqlBuilder.append("INDEX ");
		sqlBuilder.append(indexPart + " ON ");
		sqlBuilder.append(tablePart + " (");
		String separator = "";
		for(ColumnSpec columnSpec : columnListOfIndex)
		{
			boolean columnQuoted = false;
			if(columnSpec.getQuotedName() != null)
			{
				columnQuoted = columnSpec.getQuotedName().booleanValue();
			}
			
			String columnPart =  columnQuoted ? 
					" " + quotedChar() +  "" + columnSpec.getName() + "" + quotedChar() +  " " :
					" " + objectNameGuidelineFormat(schemaSpec, connection, columnSpec.getName(), "COLUMN") + " " ;
			
			sqlBuilder.append(separator + columnPart);
			separator = ",";
		}
		sqlBuilder.append(") ");
		sqlBuilder.append(tableSpaceDefinition);
	
		
		PreparedStatement createIndexStatement = null;
		try
		{
			createIndexStatement = connection.prepareStatement(sqlBuilder.toString());
			createIndexStatement.executeUpdate();
		}
		finally
		{
			if(createIndexStatement != null)
			{
				try
				{
					createIndexStatement.close();
				}
				catch(Exception e){}
			}
		}
	}
	
	@Override
	public void dropIndex
	(
		Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, 
		String indexName, boolean quoted
	)
	throws SQLException
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
		
		String indexPart =  quoted ? 
				" " + quotedChar() +  "" + indexName + "" + quotedChar() +  " " :
				" " + objectNameGuidelineFormat(schemaSpec, connection, indexName, "INDEX") + " " ;
				
		PreparedStatement createColumnStatement = null;
		try
		{
			StringBuilder sqlBuilder = new StringBuilder("DROP INDEX " + schema + "." + indexPart + " ");
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
	
	public void dropDummyColumns(Connection connection, SchemaSpec schemaSpec) throws SQLException
	{
		Map<String,String> tableIndex = new HashMap<String,String>();
		Map<String, Map<String,String>> colIndex = new HashMap<String,Map<String,String>>();
		
		if(schemaSpec.getListTableSpec() != null)
		{
			for(TableSpec tableSpec : schemaSpec.getListTableSpec())
			{
				tableIndex.put(tableSpec.getName().toUpperCase(), tableSpec.getName());
			}
		}
		
		String schema = connection.getSchema();
		
		if((schemaSpec.getDbmsSchemaName() != null) && (! schemaSpec.getDbmsSchemaName().isEmpty()))
		{
			schema = schemaSpec.getDbmsSchemaName();
		}
		
		String tbl = null;
		String col = null;
		String cat = null;
		String schem = null;
		
		ResultSet resultSet = null;
		try
		{
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getColumns(null,null,"%","%");
			while(resultSet.next())
			{
				/*
				 * TABLE_CAT 						in H2 the name of DB
				 * TABLE_SCHEM 						INFORMATION_SCHEMA / PUBLIC
				 * TABLE_NAME 						
				 * COLUMN_NAME 						column name
				 * DATA_TYPE 						SQL type from java.sql.Types (int)
				 * TYPE_NAME						SQL type - Name
				 * COLUMN_SIZE						size (int)
				 * DECIMAL_DIGITS					digits for float .... (int)
				 * NULLABLE 						nullable

				 */
				cat 	= resultSet.getString("TABLE_CAT");
				schem 	= resultSet.getString("TABLE_SCHEM");
				tbl 	= resultSet.getString("TABLE_NAME");
				col 	= resultSet.getString("COLUMN_NAME");
				
				if(schem == null)
				{
					schem = cat;
				}
				
				if(tbl == null) {tbl = "";}
				if(col == null) {col = "";}
				
				
				if(! tableIndex.containsKey(tbl.toUpperCase()))
				{
					continue;
				}
				
				if(! schem.equalsIgnoreCase(schema))
				{
					continue;
				}
				
				Map<String,String> cols = colIndex.get(tbl);
				if(cols == null)
				{
					cols = new HashMap<String,String>();
					colIndex.put(tbl, cols);
				}
				cols.put(col, col);
			}
		}
		finally 
		{
			if(resultSet != null)
			{
				try
				{
					resultSet.close();
				}
				catch (Exception e) {}
			}
		}
		
		for(Entry<String, Map<String,String>> colEntry : colIndex.entrySet())
		{
			TableSpec tableSpec = new TableSpec(colEntry.getKey(), schemaSpec);
			tableSpec.setQuotedName(true);
			
			String colName = null;
			for(String columnName : colEntry.getValue().keySet())
			{
				if(columnName.equalsIgnoreCase("SODEACDFLTCOL"))
				{
					colName = columnName;
					break;
				}
			}
			
			if((colName != null) && (colEntry.getValue().size() > 1))
			{
				dropColumn(connection, schemaSpec, tableSpec, colName, true);
			}
		}
	}
	
	public String getFunctionExpression(String function)
	{
		return function + "()";
	}
	
	public boolean tableRequiresColumn()
	{
		return false;
	}
	
	@Override
	public String objectSearchPattern(SchemaSpec schemaSpec, Connection connection, String name, boolean quoted, String type)
	{
		return quoted ? name : objectNameGuidelineFormat(schemaSpec, connection, name, type);
	}

	@Override
	public String catalogSearchPattern(SchemaSpec schemaSpec, Connection connection, String catalog)
	{
		return catalog;
	}
	
	@Override
	public String schemaSearchPattern(SchemaSpec schemaSpec, Connection connection, String schema)
	{
		return objectNameGuidelineFormat(schemaSpec, connection, schema, "SCHEMA");
	}
	
	@Override
	public String objectNameGuidelineFormat(SchemaSpec schemaSpec, Connection connection, String name, String type)
	{
		return name;
	}

	@Override
	public char quotedChar()
	{
		return '"';
	}
	
	protected IColumnType findBestColumnType(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec) throws SQLException
	{
		//boolean FALLBACK,STANDARD,SPECIFIC
		
		Applicability best = Applicability.NONE;
		IColumnType columnType = null;
		boolean typeMatch = false;
		for(IColumnType check : columnDriverList)
		{
			List<String> list = check.getTypeList();
			
			if(list == null)
			{
				continue;
			}
			
			typeMatch = false;
			
			for(String typeItem : list)
			{
				if(typeItem.equalsIgnoreCase(columnSpec.getColumntype()))
				{
					typeMatch = true;
					break;
				}
			}
			
			if(! typeMatch)
			{
				continue;
			}
			
			Applicability applicability = check.getApplicability(connection, schemaSpec, tableSpec, columnSpec, connection.getMetaData().getDatabaseProductName(), this);
			if(applicability == null)
			{
				continue;
			}
			if(applicability == Applicability.NONE)
			{
				continue;
			}
			if(columnType == null)
			{
				columnType = check;
				best = applicability;
				continue;
			}
			
			if((best == Applicability.FALLBACK) && ((applicability == Applicability.STANDARD) ||  (applicability == Applicability.SPECIFIC)))
			{
				columnType = check;
				best = applicability;
				continue;
			}
			
			if((best == Applicability.STANDARD) && (applicability == Applicability.SPECIFIC))
			{
				columnType = check;
				best = applicability;
			}
		}
		return columnType;
	}
	
	@Override
	public boolean schemaExists(Connection connection, String schemaName) throws SQLException
	{
		boolean exist = false;
		DatabaseMetaData meta = connection.getMetaData();
		ResultSet resultSet = meta.getSchemas();
		while (resultSet.next()) 
		{
			String tableSchema = resultSet.getString(1);		// TABLE_SCHEM String => schema name 
			// String tableCatalog = resultSet.getString(2);	// TABLE_CATALOG String => catalog name (may be null)
			if(tableSchema.equalsIgnoreCase(schemaName))
			{
				exist = true;
				break;
			}
	    }
		resultSet.close();
		
		return exist;
	}
	
	public void createSchema(Connection connection, String schemaName, Map<String,Object> properties) throws SQLException
	{
		String sql = "CREATE SCHEMA IF NOT EXISTS " + objectNameGuidelineFormat(null, connection, schemaName, "SCHEMA") + " AUTHORIZATION " + connection.getMetaData().getUserName();
		PreparedStatement prepStat = connection.prepareStatement(sql);
		prepStat.executeUpdate();
		prepStat.close();
	}
	
	protected boolean confirmDropSchema(Connection connection, String schemaName, Map<String,Object> properties) throws SQLException
	{
		if(properties ==  null)
		{
			throw new SQLException("no confirm informations to drop schema");
		}
		
		if(properties.get("YES_I_REALLY_WANT_DROP_SCHEMA_" + schemaName.toUpperCase()) == null)
		{
			throw new SQLException("no main confirmation to drop schema");
		}
		
		if(! ((Boolean)properties.get("YES_I_REALLY_WANT_DROP_SCHEMA_" + schemaName.toUpperCase())).booleanValue())
		{
			throw new SQLException("no main confirmation to drop schema");
		}
		
		if(properties.get("OF_COURSE_I_HAVE_A_BACKUP_OF_ALL_IMPORTANT_DATASETS") == null)
		{
			throw new SQLException("no backup confirmation of all important datasets to drop schema");
		}
		
		if(! ((Boolean)properties.get("OF_COURSE_I_HAVE_A_BACKUP_OF_ALL_IMPORTANT_DATASETS")).booleanValue())
		{
			throw new SQLException("no backup confirmation of all important datasets to drop schema");
		}
		return true;
	}
	public void dropSchema(Connection connection, String schemaName, Map<String,Object> properties) throws SQLException
	{
		if(! confirmDropSchema(connection, schemaName, properties))
		{
			throw new SQLException("you should confirm drop schema");
		}
		PreparedStatement prepStat = connection.prepareStatement("DROP SCHEMA " + objectNameGuidelineFormat(null, connection, schemaName, "SCHEMA") + " CASCADE ");
		prepStat.executeUpdate();
		prepStat.close();
		
	}

	@Override
	public Blob createBlob(Connection connection) throws SQLException
	{
		return connection.createBlob();
	}

	@Override
	public Blob getBlob(Connection connection, ResultSet resultSet, int columnIndex) throws SQLException
	{
		return resultSet.getBlob(columnIndex);
	}

	@Override
	public Blob getBlob(Connection connection, ResultSet resultSet, String columnLabel) throws SQLException
	{
		return resultSet.getBlob(columnLabel);
	}

	@Override
	public void setBlob(Connection connection, PreparedStatement preparedStatement, Blob blob, int parameterIndex) throws SQLException
	{
		if(blob == null)
		{
			preparedStatement.setNull(parameterIndex, Types.BLOB);
		}
		else
		{
			preparedStatement.setBlob(parameterIndex, blob);
		}
	}

	@Override
	public boolean requireCleanBlob(Connection connection)
	{
		return false;
	}

	@Override
	public void cleanBlob(Connection connection, Blob blob)throws SQLException{}
}
