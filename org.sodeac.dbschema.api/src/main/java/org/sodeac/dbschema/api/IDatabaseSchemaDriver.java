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

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IDatabaseSchemaDriver
{
    int HANDLE_NONE = -1;
    int HANDLE_FALLBACK = 0;
    int HANDLE_DEFAULT = 10000;
    
    String REQUIRED_DEFAULT_COLUMN = "SodeacDfltCol";
    
    enum Function
    {CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME}
    
    /**
     * Methode to request the compatibility to handle the {@link Connection} as schema driver.
     * The return value declares a priority. The provider selects the driver with highest priority.
     *
     * @param connection {@link Connection} to maintenance schema
     *
     * @return priority value declares the compatibility.
     *
     * @throws SQLException
     */
    int handle(Connection connection) throws SQLException;
    
    /**
     * getter for schema driver type / database
     *
     * @param connection underlying connection
     *
     * @return type of schema driver / database
     *
     * @throws SQLException
     */
    String getType(Connection connection) throws SQLException;
    
    /**
     * setter for complete list of column type implementations
     *
     * @param columnDriverList complete list of column type implementations
     */
    void setColumnDriverList(List<IColumnType> columnDriverList);
    
    /**
     * create a new schema
     *
     * @param connection underlying connection to create schema name
     * @param schemaName name of schema to create
     * @param properties further driver-specific informations
     *
     * @throws SQLException
     */
    void createSchema(Connection connection, String schemaName, Map<String, Object> properties) throws SQLException;
    
    /**
     * check the schema exists
     *
     * @param connection underlying connection to check the schema
     * @param schemaName name of schema to check
     *
     * @return true, if schema exists, otherwise false
     *
     * @throws SQLException
     */
    boolean schemaExists(Connection connection, String schemaName) throws SQLException;
    
    /**
     * drops a schema
     *
     * @param connection underlying connection to drop the schema
     * @param schemaName name of schema to drop
     * @param properties informations to confirm drop
     *
     * @throws SQLException
     */
    void dropSchema(Connection connection, String schemaName, Map<String, Object> properties) throws SQLException;
    
    /**
     * check existence of table by {@code tableSpec}.
     *
     * @param connection      underlying connection to check existence of table
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification to check
     * @param tableProperties properties to store working parameter
     *
     * @return true, if table exists or false if table not exists
     *
     * @throws SQLException
     */
    boolean tableExists(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, Map<String, Object> tableProperties) throws SQLException;
    
    /**
     *
     * create a table object in database
     *
     * @param connection      underlying connection to create table
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification to create table
     * @param tableProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void createTable(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, Map<String, Object> tableProperties) throws SQLException;
    
    /**
     *
     * check existence of tables primary key by {@code tableSpec}.
     *
     * @param connection      underlying connection to check existence of primary key
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification to check
     * @param tableProperties properties to store working parameter
     *
     * @return true, if primary exists or false if primary key not exists
     *
     * @throws SQLException
     */
    boolean primaryKeyExists(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, Map<String, Object> tableProperties) throws SQLException;
    
    /**
     * create primary key
     *
     * @param connection      underlying connection to create primary key
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification to create primary key
     * @param tableProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void setPrimaryKey(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, Map<String, Object> tableProperties) throws SQLException;
    
    /**
     *
     * check existence of column by {@code columnSpec}.
     *
     * @param connection       underlying connection to check existence of column
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to check
     * @param columnProperties properties to store working parameter
     *
     * @return true if column exists, otherwise false
     *
     * @throws SQLException
     */
    boolean columnExists(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     *
     * determine type of column
     *
     * @param connection       underlying connection to check valid type of existing column
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to check
     * @param columnProperties properties with working parameter
     *
     * @return null, if driver can not determine type of column, otherwise type
     *
     * @throws SQLException
     */
    String determineColumnType(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     * create column
     *
     * @param connection       connection underlying connection to create column
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to create
     * @param columnProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void createColumn(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     * drop column
     *
     * @param connection underlying connection to drop column
     * @param schemaSpec hole schema specification
     * @param tableSpec  table specification of columns table
     * @param columnName name of column to drop
     * @param quoted     use column name as quoted object name
     *
     * @throws SQLException
     */
    void dropColumn(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, String columnName, boolean quoted) throws SQLException;
    
    /**
     * check column properties (default-value, type, nullable)
     *
     * @param connection       underlying connection to check column
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to check
     * @param columnProperties properties to store working parameter
     *
     * @return return true column setup is valid, otherwise false
     *
     * @throws SQLException
     */
    boolean isValidColumnProperties(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     * set valid column properties (default-value, type, nullable)
     *
     * @param connection       connection underlying connection to setup column
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification for setup
     * @param columnProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void setValidColumnProperties(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     *
     * check valid created foreign key
     *
     * @param connection       underlying connection to check foreign key
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to check foreign key
     * @param columnProperties properties to store working parameter
     *
     * @return true, if foreign key is created
     *
     * @throws SQLException
     */
    boolean isValidForeignKey(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     * create or update foreign key setting for {@code columnSpec}
     *
     * @param connection       underlying connection to set foreign key
     * @param schemaSpec       hole schema specification
     * @param tableSpec        table specification of columns table
     * @param columnSpec       column specification to set foreign key
     * @param columnProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void setValidForeignKey(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, ColumnSpec columnSpec, Map<String, Object> columnProperties) throws SQLException;
    
    /**
     * drop foreign key
     *
     * @param connection underlying connection to drop foreign key
     * @param schemaSpec hole schema specification
     * @param tableSpec  table specification of columns table
     * @param keyName    name of foreign key to drop
     * @param quoted     use key name as quoted object name
     *
     * @throws SQLException
     */
    void dropForeignKey(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, String keyName, boolean quoted) throws SQLException;
    
    /**
     * check valid index setup
     *
     * @param connection      underlying connection to check index
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification of index
     * @param indexSpec       index specification
     * @param indexProperties properties to store working parameter
     *
     * @return true, if index has valid setup, otherwise false
     *
     * @throws SQLException
     */
    boolean isValidIndex(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, IndexSpec indexSpec, Map<String, Object> indexProperties) throws SQLException;
    
    /**
     * setup valid index by {@code indexSpec}
     *
     * @param connection      underlying connection to setup index
     * @param schemaSpec      hole schema specification
     * @param tableSpec       table specification of index
     * @param indexSpec       index specification
     * @param indexProperties properties to store working parameter
     *
     * @throws SQLException
     */
    void setValidIndex(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, IndexSpec indexSpec, Map<String, Object> indexProperties) throws SQLException;
    
    /**
     * drop index with indexName
     *
     * @param connection underlying connection to drop index
     * @param schemaSpec hole schema specification
     * @param tableSpec  table specification of index
     * @param indexName  name of index to drop
     * @param quoted     use index name as quoted object name
     *
     * @throws SQLException
     */
    void dropIndex(Connection connection, SchemaSpec schemaSpec, TableSpec tableSpec, String indexName, boolean quoted) throws SQLException;
    
    /**
     * clean schema from columns created with table-objects by dbms can not create tables without columns
     *
     * @param connection underlying connection to clean from dummy columns
     * @param schemaSpec hole schema specification
     *
     * @throws SQLException
     */
    void dropDummyColumns(Connection connection, SchemaSpec schemaSpec) throws SQLException;
    
    /**
     * convert function name to  function syntax
     *
     * @param function name of function
     *
     * @return valid function syntax
     */
    String getFunctionExpression(String function);
    
    /**
     *
     *
     * @return true, of dbms requires a column on table creation, otherwise false
     */
    boolean tableRequiresColumn();
    
    /**
     *
     * @param schemaSpec hole schema specification
     * @param connection underlying connection
     * @param catalog    catalog name
     *
     * @return catalog filter object for jdbc meta api
     */
    String catalogSearchPattern(SchemaSpec schemaSpec, Connection connection, String catalog);
    
    /**
     *
     * @param schemaSpec hole schema specification
     * @param connection underlying connection
     * @param schema     schema name
     *
     * @return schema filter object for jdbc meta api
     */
    String schemaSearchPattern(SchemaSpec schemaSpec, Connection connection, String schema);
    
    /**
     *
     * @param schemaSpec hole schema specification
     * @param connection underlying connection
     * @param name       object's name
     * @param quoted     use name as quoted object name
     * @param type       object type (TABLE,COLUMN ...)
     *
     * @return filter object for jdbc meta api
     */
    String objectSearchPattern(SchemaSpec schemaSpec, Connection connection, String name, boolean quoted, String type);
    
    /**
     * convert object name (e.g. table name, column name, key name ) to dbms conform name
     *
     * @param schemaSpec hole schema specification
     * @param connection underlying connection
     * @param name       object's name
     * @param type       object type
     *
     * @return jdbc specific object name
     */
    String objectNameGuidelineFormat(SchemaSpec schemaSpec, Connection connection, String name, String type);
    
    /**
     *
     * @return character to quote an identifier
     */
    char quotedChar();
    
    /**
     * create new blob to store binary data
     *
     * @param connection underlying connection
     *
     * @return blob
     *
     * @throws SQLException
     */
    Blob createBlob(Connection connection) throws SQLException;
    
    /**
     * fetch existing blob from resultset
     *
     * @param connection  underlying connection
     * @param resultSet   resultset to fetch blob
     * @param columnIndex column index for blob (start with 1)
     *
     * @return blob
     *
     * @throws SQLException
     */
    Blob getBlob(Connection connection, ResultSet resultSet, int columnIndex) throws SQLException;
    
    /**
     * fetch existing blob from resultset
     *
     * @param connection  underlying connection
     * @param resultSet   resultset to fetch blob
     * @param columnLabel column label for blob
     *
     * @return blob
     *
     * @throws SQLException
     */
    Blob getBlob(Connection connection, ResultSet resultSet, String columnLabel) throws SQLException;
    
    /**
     * store blob into database
     *
     * @param connection        underlying connection
     * @param preparedStatement prepared statement to store blob
     * @param blob              blob to store
     * @param parameterIndex    parameter index in prepared statement (start with 1)
     *
     * @throws SQLException
     */
    void setBlob(Connection connection, PreparedStatement preparedStatement, Blob blob, int parameterIndex) throws SQLException;
    
    /**
     * get necessity to manually clean blobs
     *
     * @param connection underlying connection
     *
     * @return true, if dbms does no clean content automatically after removing blob row or similar actions, otherwise false
     */
    boolean requireCleanBlob(Connection connection);
    
    /**
     *
     * clean blob content, if dbms does no clean content automatically after removing blob row or similar actions
     *
     * @param connection underlying connection
     * @param blob       blob to clean
     */
    void cleanBlob(Connection connection, Blob blob) throws SQLException;
}
