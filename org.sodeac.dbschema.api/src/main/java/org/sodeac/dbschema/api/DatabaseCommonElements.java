/*******************************************************************************
 * Copyright (c) 2017, 2018 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.dbschema.api;

/**
 * Defines default sodeac database objects
 * 
 * @author Sebastian Palarus
 *
 */
public interface DatabaseCommonElements 
{
	/**
	 * Table sodeac_context should store objects which isolate dataset like clients or customers
	 */
	public static final String TABLE_CONTEXT 		= "SODEAC_CONTEXT"			;
	
	/**
	 * Table sodeac_context_group contains possible type of context entries
	 */
	public static final String TABLE_CONTEXT_GROUP 	= "SODEAC_CONTEXT_GROUP"	;
	
	/**
	 * ID, Primary Key of dataset
	 */
	public static final String ID 					= "ID"						;
	
	/**
	 * related conext of dataset
	 */
	public static final String CONTEXT 				= "CONTEXT_ID"				;
	
	/**
	 * name of dataset
	 */
	public static final String NAME 				= "OBJ_NAME"				;
	
	/**
	 * key of a property
	 */
	public static final String KEY 					= "OBJ_KEY"					;
	
	/**
	 * value of a property
	 */
	public static final String VALUE 				= "OBJ_VALUE"				;
	
	/**
	 * format of data
	 */
	public static final String FORMAT 				= "OBJ_FORMAT"				;
	
	/**
	 * type of data
	 */
	public static final String TYPE 				= "OBJ_TYPE"				;
	
	/**
	 * dataset is enabled (or not)
	 */
	public static final String ENABLED 				= "OBJ_ENABLED"				;
	
	/**
	 * dataset is deleted (or not)
	 */
	public static final String DELETED 				= "OBJ_DELETED"				;
	
	/**
	 * related domain of dataset
	 */
	public static final String DOMAIN 				= "OBJ_DOMAIN"				;
	
	/**
	 * related entity type of dataset
	 */
	public static final String ENTITY_TYPE 			= "OBJ_ENTITY_TYPE"			;
	
	/**
	 * related dataset id of entity
	 */
	public static final String ENTITY_ID 			= "OBJ_ENTITY_ID"			;
	
	/**
	 * related aggregate
	 */
	public static final String AGGREGATE 			= "AGGREGATE"				;
	
	/**
	 * context type is a free definition of context type
	 */
	public static final String CONTEXT_TYPE 		= "CONTEXT_TYPE"			;
	
	/**
	 * the version of dataset in table. At every change the affected rows get assigned a new sequence.
	 */
	public static final String TABLE_VERSION 		= "TABLE_VERSION"			;
	
	/**
	 * uuid of a dataset. At every change the affected dataset get assigned a new uuid.
	 */
	public static final String DATASET_ID 			= "DATASET_ID"				;
	
	/**
	 * timestamp of last change.
	 */
	public static final String PERSIST_TIMESTAMP 	= "PERSIST_TIMESTAMP"		;
	
	/**
	 * source of last change (users, services ....)
	 */
	public static final String PERSIST_SOURCE 		= "PERSIST_SOURCE"			;
	
	/**
	 * type of source of last change
	 */
	public static final String PERSIST_TYPE 		= "PERSIST_TYPE"			;
	
	/**
	 * Source node of last change (uuid of cluster node)
	 */
	public static final String PERSIST_NODE 		= "PERSIST_NODE"			;
	
	/**
	 * widget type to handle this dataset
	 */
	public static final String WIDGET_TYPE	 		= "WIDGET_TYPE"				;
}
