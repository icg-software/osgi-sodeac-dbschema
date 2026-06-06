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
 * Specification of column
 *
 * @author Sebastian Palarus
 *
 */
public class ColumnSpec
{
    private String name = null;
    private String columntype = null;
    private boolean nullable = true;
    private int size = 0;
    private String defaultValue = null;
    private boolean defaultValueByFunction = false;
    private PrimaryKeySpec primaryKey = null;
    private ForeignKeySpec foreignKey = null;
    private TableSpec tableSpec = null;
    private Boolean quotedName = null;
    
    /**
     * Creates new column-specification for a {@code tableSpec} with {@code name} and {@code columntype}
     *
     * @param tableSpec  table specification
     * @param name       column-name
     * @param columntype string represent of {@link IColumnType}
     */
    public ColumnSpec(final TableSpec tableSpec, final String name, final String columntype)
    {
        super();
        this.name = name;
        this.columntype = columntype;
        this.tableSpec = tableSpec;
    }
    
    /**
     * Creates new column-specification for a {@code tableSpec} with {@code name} and {@code columntype} and given capability to store null values
     *
     * @param tableSpec  table specification
     * @param name       column-name
     * @param columntype string represent of {@link IColumnType}
     * @param nullable   capability to store null values
     */
    public ColumnSpec(final TableSpec tableSpec, final String name, final String columntype, final boolean nullable)
    {
        super();
        this.name = name;
        this.columntype = columntype;
        this.nullable = nullable;
        this.tableSpec = tableSpec;
    }
    
    /**
     * Creates new column-specification for a {@code tableSpec} with {@code name} and {@code columntype} and given capability to store null values and column size
     *
     * @param tableSpec  table specification
     * @param name       column-name
     * @param columntype string represent of {@link IColumnType}
     * @param nullable   capability to store null values
     * @param size       column-size
     */
    public ColumnSpec(final TableSpec tableSpec, final String name, final String columntype, final boolean nullable, final int size)
    {
        super();
        this.name = name;
        this.columntype = columntype;
        this.size = size;
        this.nullable = nullable;
        this.tableSpec = tableSpec;
    }
    
    /**
     * Getter for nullable property
     *
     * @return capability to store null values
     */
    public boolean getNullable()
    {
        return this.nullable;
    }
    
    /**
     * Setter for nullable property
     *
     * @param nullable capability to store null values
     *
     * @return column specification
     */
    public ColumnSpec setNullable(final boolean nullable)
    {
        this.nullable = nullable;
        return this;
    }
    
    /**
     * Getter for name of column
     *
     * @return column name
     */
    public String getName()
    {
        return this.name;
    }
    
    /**
     * Getter for column type
     *
     * @return string represent of {@link IColumnType}
     */
    public String getColumntype()
    {
        return this.columntype;
    }
    
    /**
     * Getter for column size
     *
     * @return column size
     */
    public int getSize()
    {
        return this.size;
    }
    
    /**
     * Setter for column size
     *
     * @param size column size
     *
     * @return column specification
     */
    public ColumnSpec setSize(final int size)
    {
        this.size = size;
        return this;
    }
    
    /**
     * Getter for default value
     *
     * @return default value of column
     */
    public String getDefaultValue()
    {
        return this.defaultValue;
    }
    
    /**
     * Setter for default value
     *
     * @param defaultValue default value of column
     *
     * @return column specification
     */
    public ColumnSpec setDefaultValue(final String defaultValue)
    {
        this.defaultValue = defaultValue;
        return this;
    }
    
    /**
     * Getter for primary key specification
     *
     * @return primary key specification
     */
    public PrimaryKeySpec getPrimaryKey()
    {
        return this.primaryKey;
    }
    
    /**
     * define column as primary key with default constraint name and key name
     *
     * @return column specification
     */
    public ColumnSpec setPrimaryKey()
    {
        this.primaryKey = new PrimaryKeySpec("PK_" + this.tableSpec.getName().toUpperCase(), "PKX_" + this.tableSpec.getName().toUpperCase());
        return this;
    }
    
    /**
     * define column as primary key.
     *
     * @param constraintName name of primary key
     * @param indexName      name of key ( for dbms creates index additionally to primary key constraint)
     *
     * @return column spec
     */
    public ColumnSpec setPrimaryKey(String constraintName, String indexName)
    {
        if (constraintName == null)
        {
            constraintName = "PK_" + this.tableSpec.getName().toUpperCase();
        }
        if (indexName == null)
        {
            indexName = "PKX_" + this.tableSpec.getName().toUpperCase();
        }
        this.primaryKey = new PrimaryKeySpec(constraintName, indexName);
        return this;
    }
    
    /**
     * define column as primary key.
     *
     * @param constraintName name of primary key
     * @param indexName      name of index ( for dbms creates index additionally to primary key constraint)
     * @param quotedName     quoted name property (index and constraint name)
     *
     * @return column specification
     */
    public ColumnSpec setPrimaryKey(String constraintName, String indexName, final Boolean quotedName)
    {
        if (constraintName == null)
        {
            constraintName = "PK_" + this.tableSpec.getName().toUpperCase();
        }
        if (indexName == null)
        {
            indexName = "PKX_" + this.tableSpec.getName().toUpperCase();
        }
        this.primaryKey = new PrimaryKeySpec(constraintName, indexName);
        this.primaryKey.setQuotedName(quotedName);
        return this;
    }
    
    /**
     * define column as primary key.
     *
     * @param constraintName name of primary key
     * @param indexName      name of index ( for dbms creates index additionally to primary key constraint)
     * @param quotedName     quoted name property (index and constraint name)
     * @param tableSpace     tablespace for key (must exist). Once the primary key is created, the tablespace will not be adjusted anymore.
     *
     * @return column specification
     */
    public ColumnSpec setPrimaryKey(String constraintName, String indexName, final Boolean quotedName, final String tableSpace)
    {
        if (constraintName == null)
        {
            constraintName = "PK_" + this.tableSpec.getName().toUpperCase();
        }
        if (indexName == null)
        {
            indexName = "PKX_" + this.tableSpec.getName().toUpperCase();
        }
        this.primaryKey = new PrimaryKeySpec(constraintName, indexName);
        this.primaryKey.setTableSpace(tableSpace);
        this.primaryKey.setQuotedName(quotedName);
        return this;
    }
    
    /**
     * Getter for foreign key specification
     *
     * @return foreign key specification
     */
    public ForeignKeySpec getForeignKey()
    {
        return this.foreignKey;
    }
    
    /**
     * define column as foreign key.
     *
     * @param constraintName      constraint name of foreign key
     * @param referencedTableName referenced table
     *
     * @return column specification
     */
    public ColumnSpec setForeignKey(final String constraintName, final String referencedTableName)
    {
        this.foreignKey = new ForeignKeySpec(constraintName, referencedTableName);
        return this;
    }
    
    /**
     * define column as foreign key.
     *
     * @param constraintName       constraint name of foreign key
     * @param referencedTableName  referenced table
     * @param referencedColumnName referenced column
     *
     * @return column specification
     */
    public ColumnSpec setForeignKey(final String constraintName, final String referencedTableName, final String referencedColumnName)
    {
        this.foreignKey = new ForeignKeySpec(constraintName, referencedTableName, referencedColumnName);
        return this;
    }
    
    /**
     * Getter for quoted name property. If column name is quoted, the name is managed case-sensitive and can include keywords.
     *
     * @return quoted name property
     */
    public Boolean getQuotedName()
    {
        return this.quotedName;
    }
    
    /**
     * Setter for quoted name property. If column name is quoted, the name is managed case-sensitive and can include keywords.
     *
     * @param quotedName quoted name property
     *
     * @return column specification
     */
    public ColumnSpec setQuotedName(final Boolean quotedName)
    {
        this.quotedName = quotedName;
        return this;
    }
    
    /**
     * Getter for parent table specification
     *
     * @return table specification
     */
    public TableSpec getTableSpec()
    {
        return this.tableSpec;
    }
    
    /**
     * Getter for parent table specification with better expression for java fluent api
     *
     * @return table specification
     */
    public TableSpec endColumnDefinition()
    {
        return this.tableSpec;
    }
    
    /**
     * Getter for default-value-by-function-property. If this property is true, the default-value is managed as function/procedure
     *
     * @return default-value-by-function-property
     */
    public boolean getDefaultValueByFunction()
    {
        return this.defaultValueByFunction;
    }
    
    /**
     * Setter for default-value-by-function-property. If this property is true, the default-value is managed as function/procedure
     *
     * @param defaultValueByFunction default-value-by-function-property
     *
     * @return column specification
     */
    public ColumnSpec setDefaultValueByFunction(final boolean defaultValueByFunction)
    {
        this.defaultValueByFunction = defaultValueByFunction;
        return this;
    }
    
    @Override
    public String toString()
    {
        return "ColumnSpec " + this.name;
    }
    
}
