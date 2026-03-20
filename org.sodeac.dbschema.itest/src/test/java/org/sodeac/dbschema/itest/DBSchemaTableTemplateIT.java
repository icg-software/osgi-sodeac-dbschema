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
package org.sodeac.dbschema.itest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.Hashtable;

import org.easymock.IMocksControl;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.dbschema.api.ActionType;
import org.sodeac.dbschema.api.DatabaseCommonElements;
import org.sodeac.dbschema.api.DefaultSodeacSchemaTemplate;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.IDatabaseSchemaUpdateListener;
import org.sodeac.dbschema.api.ObjectType;
import org.sodeac.dbschema.api.PhaseType;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaTableTemplateIT extends AbstractDBSchemaIT
{
    public DBSchemaTableTemplateIT(final String dbType) { super(dbType); }

    String table1Name = "TblWithColsByTemplate";

    @Test
    public void test001300SodeacDefaultTemplate() throws SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);
        spec.applyTemplate(DefaultSodeacSchemaTemplate.class);

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);
        spec.applyTemplate(DefaultSodeacSchemaTemplate.class);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name).applyTemplate(DefaultSodeacSchemaTemplate.class);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.ID));

        final Dictionary<ObjectType, Object> table1ColumnContextDictionary = new Hashtable<>();
        table1ColumnContextDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnContextDictionary.put(ObjectType.TABLE, table1);
        table1ColumnContextDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.CONTEXT));

        final Dictionary<ObjectType, Object> table1ColumnContextTypeDictionary = new Hashtable<>();
        table1ColumnContextTypeDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnContextTypeDictionary.put(ObjectType.TABLE, table1);
        table1ColumnContextTypeDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.CONTEXT_TYPE));

        final Dictionary<ObjectType, Object> table1ColumnTableVersionDictionary = new Hashtable<>();
        table1ColumnTableVersionDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnTableVersionDictionary.put(ObjectType.TABLE, table1);
        table1ColumnTableVersionDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.TABLE_VERSION));

        final Dictionary<ObjectType, Object> table1ColumnDatasetIdDictionary = new Hashtable<>();
        table1ColumnDatasetIdDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnDatasetIdDictionary.put(ObjectType.TABLE, table1);
        table1ColumnDatasetIdDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.DATASET_ID));

        final Dictionary<ObjectType, Object> table1ColumnPersistTSDictionary = new Hashtable<>();
        table1ColumnPersistTSDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistTSDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistTSDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_TIMESTAMP));

        final Dictionary<ObjectType, Object> table1ColumnPersistSrcDictionary = new Hashtable<>();
        table1ColumnPersistSrcDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistSrcDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistSrcDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_SOURCE));

        final Dictionary<ObjectType, Object> table1ColumnPersistTpDictionary = new Hashtable<>();
        table1ColumnPersistTpDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistTpDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistTpDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_TYPE));

        final Dictionary<ObjectType, Object> table1ColumnPersistNdDictionary = new Hashtable<>();
        table1ColumnPersistNdDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistNdDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistNdDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_NODE));

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_NULLABLE, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);

        // table1 create keys/indices

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.TABLE_PRIMARY_KEY, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        // FK

        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.PRE, connection, databaseID, table1ColumnContextDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.COLUMN_FOREIGN_KEY, PhaseType.POST, connection, databaseID, table1ColumnContextDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }

    @Test
    public void test001301SodeacDefaultTemplateAgain() throws SQLException, ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final String databaseID = DOMAIN;

        final IMocksControl ctrl = this.support.createControl();
        final IDatabaseSchemaUpdateListener updateListenerMock = ctrl.createMock(IDatabaseSchemaUpdateListener.class);

        ctrl.checkOrder(true);

        // create spec
        SchemaSpec spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);
        spec.applyTemplate(DefaultSodeacSchemaTemplate.class);

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        spec = new SchemaSpec(databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);
        spec.applyTemplate(DefaultSodeacSchemaTemplate.class);

        // prepare spec for simulation
        final Dictionary<ObjectType, Object> schemaDictionary = new Hashtable<>();
        schemaDictionary.put(ObjectType.SCHEMA, spec);
        spec.addUpdateListener(updateListenerMock);

        final TableSpec table1 = spec.addTable(this.table1Name).applyTemplate(DefaultSodeacSchemaTemplate.class);

        // prepare table for simulation
        final Dictionary<ObjectType, Object> table1Dictionary = new Hashtable<>();
        table1Dictionary.put(ObjectType.SCHEMA, spec);
        table1Dictionary.put(ObjectType.TABLE, table1);
        table1.addUpdateListener(updateListenerMock);

        final Dictionary<ObjectType, Object> table1ColumnPKDictionary = new Hashtable<>();
        table1ColumnPKDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPKDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPKDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.ID));

        final Dictionary<ObjectType, Object> table1ColumnContextDictionary = new Hashtable<>();
        table1ColumnContextDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnContextDictionary.put(ObjectType.TABLE, table1);
        table1ColumnContextDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.CONTEXT));

        final Dictionary<ObjectType, Object> table1ColumnContextTypeDictionary = new Hashtable<>();
        table1ColumnContextTypeDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnContextTypeDictionary.put(ObjectType.TABLE, table1);
        table1ColumnContextTypeDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.CONTEXT_TYPE));

        final Dictionary<ObjectType, Object> table1ColumnTableVersionDictionary = new Hashtable<>();
        table1ColumnTableVersionDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnTableVersionDictionary.put(ObjectType.TABLE, table1);
        table1ColumnTableVersionDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.TABLE_VERSION));

        final Dictionary<ObjectType, Object> table1ColumnDatasetIdDictionary = new Hashtable<>();
        table1ColumnDatasetIdDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnDatasetIdDictionary.put(ObjectType.TABLE, table1);
        table1ColumnDatasetIdDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.DATASET_ID));

        final Dictionary<ObjectType, Object> table1ColumnPersistTSDictionary = new Hashtable<>();
        table1ColumnPersistTSDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistTSDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistTSDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_TIMESTAMP));

        final Dictionary<ObjectType, Object> table1ColumnPersistSrcDictionary = new Hashtable<>();
        table1ColumnPersistSrcDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistSrcDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistSrcDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_SOURCE));

        final Dictionary<ObjectType, Object> table1ColumnPersistTpDictionary = new Hashtable<>();
        table1ColumnPersistTpDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistTpDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistTpDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_TYPE));

        final Dictionary<ObjectType, Object> table1ColumnPersistNdDictionary = new Hashtable<>();
        table1ColumnPersistNdDictionary.put(ObjectType.SCHEMA, spec);
        table1ColumnPersistNdDictionary.put(ObjectType.TABLE, table1);
        table1ColumnPersistNdDictionary.put(ObjectType.COLUMN, table1.getColumn(DatabaseCommonElements.PERSIST_NODE));

        // simulate listener

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);

        // table creation

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.PRE, connection, databaseID, table1Dictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.TABLE, PhaseType.POST, connection, databaseID, table1Dictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPKDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPKDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnContextTypeDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnTableVersionDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnDatasetIdDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTSDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistSrcDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistTpDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.PRE, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.COLUMN, PhaseType.POST, connection, databaseID, table1ColumnPersistNdDictionary, driver, null);

        // convert schema

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.PRE, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.UPDATE, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);
        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA_CONVERT_SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        updateListenerMock.onAction(ActionType.CHECK, ObjectType.SCHEMA, PhaseType.POST, connection, databaseID, schemaDictionary, driver, null);

        ctrl.replay();

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        ctrl.verify();
    }
}
