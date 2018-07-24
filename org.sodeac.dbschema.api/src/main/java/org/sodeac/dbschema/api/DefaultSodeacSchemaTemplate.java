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
package org.sodeac.dbschema.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Dictionary;

/**
 * Templates for common sodeac database objects
 * 
 * @author Sebastian Palarus
 *
 */
public class DefaultSodeacSchemaTemplate implements ITableTemplate,ISchemaTemplate, IDatabaseSchemaUpdateListener
{

	/**
	 * Creates sodeac common column settings in table
	 */
	@Override
	public void tableTemplateApply(TableSpec tableSpec)
	{
		tableSpec.addUpdateListener(this);
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.ID))
		{
			tableSpec.addColumn(DatabaseCommonElements.ID, IColumnType.ColumnType.CHAR.toString(), false, 36).setPrimaryKey();
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.CONTEXT))
		{
			tableSpec.addColumn(DatabaseCommonElements.CONTEXT, IColumnType.ColumnType.CHAR.toString(), true,36)
			.setForeignKey("FKCX_" +tableSpec.getName().toUpperCase() , DatabaseCommonElements.TABLE_CONTEXT, DatabaseCommonElements.ID);
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.CONTEXT_TYPE))
		{
			tableSpec.addColumn(DatabaseCommonElements.CONTEXT_TYPE, IColumnType.ColumnType.INTEGER.toString(), true);
		}
		
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.TABLE_VERSION))
		{
			tableSpec.addColumn(DatabaseCommonElements.TABLE_VERSION, IColumnType.ColumnType.BIGINT.toString(),true).setDefaultValue("0");
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.DATASET_ID))
		{
			tableSpec.addColumn(DatabaseCommonElements.DATASET_ID, IColumnType.ColumnType.CHAR.toString(), true,36);
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.PERSIST_TIMESTAMP))
		{
			tableSpec.addColumn(DatabaseCommonElements.PERSIST_TIMESTAMP, IColumnType.ColumnType.TIMESTAMP.toString(), true);
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.PERSIST_SOURCE))
		{
			tableSpec.addColumn(DatabaseCommonElements.PERSIST_SOURCE, IColumnType.ColumnType.CHAR.toString(), true,36);
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.PERSIST_TYPE))
		{
			tableSpec.addColumn(DatabaseCommonElements.PERSIST_TYPE, IColumnType.ColumnType.CHAR.toString(), true,128);
		}
		
		if(! tableSpec.containsColumns(DatabaseCommonElements.PERSIST_NODE))
		{
			tableSpec.addColumn(DatabaseCommonElements.PERSIST_NODE, IColumnType.ColumnType.CHAR.toString(), true,36);
		}
	}

	/**
	 * Creates sodeac common tables in schema
	 */
	@Override
	public void schemaTemplateApply(SchemaSpec schemaSpec)
	{
		/*
		 * Table ContextGroup
		 */
				
		schemaSpec.addTable(DatabaseCommonElements.TABLE_CONTEXT_GROUP)
			
			.addColumn(DatabaseCommonElements.ID		, IColumnType.ColumnType.CHAR.toString()	, false	, 36	).setPrimaryKey().endColumnDefinition()
			.addColumn(DatabaseCommonElements.NAME		, IColumnType.ColumnType.VARCHAR.toString()	, false	, 108	).endColumnDefinition()
			.addColumn(DatabaseCommonElements.KEY		, IColumnType.ColumnType.VARCHAR.toString()	, false	, 108	).endColumnDefinition()
			
			.addColumnIndex("UNQ_" + DatabaseCommonElements.TABLE_CONTEXT_GROUP + "_" + DatabaseCommonElements.KEY	, DatabaseCommonElements.KEY	,true).endColumnIndexDefinition()
			
		.endTableSpec()
		
		/*
		 * Table Context
		 */
		.addTable(DatabaseCommonElements.TABLE_CONTEXT)
		
			.addColumn(DatabaseCommonElements.ID							, IColumnType.ColumnType.CHAR.toString()	, false	, 36	).setPrimaryKey().endColumnDefinition()
			
			.addColumn(DatabaseCommonElements.TABLE_CONTEXT_GROUP + "_ID"	, IColumnType.ColumnType.VARCHAR.toString()	, false	, 36	)
				.setForeignKey("FK1_" + DatabaseCommonElements.TABLE_CONTEXT, DatabaseCommonElements.TABLE_CONTEXT_GROUP, DatabaseCommonElements.ID).endColumnDefinition()
			
			.addColumn(DatabaseCommonElements.NAME							, IColumnType.ColumnType.VARCHAR.toString()	, false	, 108	).endColumnDefinition()
			.addColumn(DatabaseCommonElements.KEY							, IColumnType.ColumnType.VARCHAR.toString()	, false	, 108	).endColumnDefinition()
		
		.addColumnIndex("UNQ_" + DatabaseCommonElements.TABLE_CONTEXT + "_" + DatabaseCommonElements.KEY	, DatabaseCommonElements.KEY	,true).endColumnIndexDefinition()
		
		.endTableSpec();
	}

	@Override
	public void onAction
	(
		ActionType actionType, ObjectType objectType, PhaseType phaseType, Connection connection,
		String databaseID, Dictionary<ObjectType, Object> objects, IDatabaseSchemaDriver driver,
		Exception exception
	) throws SQLException
	{
	}
	
}
