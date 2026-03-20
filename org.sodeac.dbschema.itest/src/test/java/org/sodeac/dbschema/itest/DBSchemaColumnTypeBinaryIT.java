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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.sodeac.dbschema.api.IColumnType;
import org.sodeac.dbschema.api.IDatabaseSchemaDriver;
import org.sodeac.dbschema.api.SchemaSpec;
import org.sodeac.dbschema.api.TableSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DBSchemaColumnTypeBinaryIT extends AbstractDBSchemaIT
{

    private final String databaseID = "TESTDOMAIN";
    private final String table1Name = "TableColBin";
    private final String columnBinaryName = "col_binary";
    private final String columnBlobName = "col_blob";

    public DBSchemaColumnTypeBinaryIT(final String dbType) { super(dbType); }

    @Test
    public void test000700binarySimpleTest() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;

        // create spec
        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        final TableSpec table1 = spec.addTable(this.table1Name);

        table1.addColumn("id", IColumnType.ColumnType.CHAR.toString(), false, 36);
        table1.addColumn(this.columnBinaryName, IColumnType.ColumnType.BINARY.toString());
        table1.addColumn(this.columnBlobName, IColumnType.ColumnType.BLOB.toString());

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        PreparedStatement prepStat = null;
        final ResultSet rset = null;
        try
        {
            connection.setAutoCommit(false);

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id) values (?)");
            prepStat.setString(1, UUID.randomUUID().toString());
            prepStat.executeUpdate();
            prepStat.close();
            connection.commit();

        }
        finally
        {
            try { rset.close(); }catch (final Exception e) { }
            try { prepStat.close(); }catch (final Exception e) { }
        }
    }

    @Test
    public void test000701testBinaryBytes() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b[i] = (byte) (i + 10);
        }

        final String id = UUID.randomUUID().toString();

        PreparedStatement prepStat = null;

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_binary) values (?,?) ");
            prepStat.setString(1, id);
            prepStat.setBytes(2, b);
            prepStat.executeUpdate();
            connection.commit();
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        ResultSet rset = null;
        prepStat = null;
        byte[] testByte = null;

        try
        {
            prepStat = connection.prepareStatement("select col_binary from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id);

            rset = prepStat.executeQuery();
            rset.next();
            testByte = rset.getBytes(1);
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        assertEquals("byte length should be correct", b.length, testByte.length);
        for (int i = 0; i < b.length; i++)
        {
            assertEquals("byte should be correct", b[i], testByte[i]);
        }

        connection.setAutoCommit(ac);
    }

    @Test
    public void test000702testBinaryStream() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b[i] = (byte) (i + 20);
        }

        final String id = UUID.randomUUID().toString();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);

        PreparedStatement prepStat = null;

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_binary) values (?,?) ");
            prepStat.setString(1, id);
            prepStat.setBinaryStream(2, bais);
            prepStat.executeUpdate();
            connection.commit();

            bais.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        ResultSet rset = null;
        prepStat = null;
        byte[] testByte = null;
        InputStream is = null;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);

        try
        {
            prepStat = connection.prepareStatement("select col_binary from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id);

            rset = prepStat.executeQuery();
            rset.next();
            is = rset.getBinaryStream(1);

            final byte[] buf = new byte[27];
            int len;
            while ((len = is.read(buf)) > 0)
            {
                baos.write(buf, 0, len);
            }
            is.close();
            baos.close();
            testByte = baos.toByteArray();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        assertEquals("byte length should be correct", b.length, testByte.length);
        for (int i = 0; i < b.length; i++)
        {
            assertEquals("byte should be correct", b[i], testByte[i]);
        }

        connection.setAutoCommit(ac);
    }

    @Test
    public void test000703testBlobInsertAndRead() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        final TableSpec table1 = spec.addTable(this.table1Name);

        table1.addColumn("id", IColumnType.ColumnType.CHAR.toString(), false, 36);
        table1.addColumn(this.columnBinaryName, IColumnType.ColumnType.BINARY.toString());
        table1.addColumn(this.columnBlobName, IColumnType.ColumnType.BLOB.toString());

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b[i] = (byte) (i + 20);
        }

        final String id = UUID.randomUUID().toString();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);

        PreparedStatement prepStat = null;

        Blob blob = driver.createBlob(connection);
        final OutputStream os = blob.setBinaryStream(1);

        final byte[] buf = new byte[27];
        int len;
        while ((len = bais.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();
            connection.commit();

            bais.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        ResultSet rset = null;
        prepStat = null;
        byte[] testByte = null;
        InputStream is = null;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
            is = blob.getBinaryStream();

            while ((len = is.read(buf)) > 0)
            {
                baos.write(buf, 0, len);
            }
            is.close();
            baos.close();
            testByte = baos.toByteArray();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        blob.free();

        assertEquals("byte length should be correct", b.length, testByte.length);
        for (int i = 0; i < b.length; i++)
        {
            assertEquals("byte should be correct", b[i], testByte[i]);
        }

        connection.setAutoCommit(ac);
    }

    @Test
    public void test000704testBlobCopyByReference() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b1 = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b1[i] = (byte) (i + 20);
        }

        final String id1 = UUID.randomUUID().toString();
        final String id2 = UUID.randomUUID().toString();

        ByteArrayInputStream bais1 = new ByteArrayInputStream(b1);

        PreparedStatement prepStat = null;

        Blob blob = driver.createBlob(connection);
        OutputStream os = blob.setBinaryStream(1);

        final byte[] buf = new byte[27];
        int len;
        while ((len = bais1.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais1.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id1);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();
            connection.commit();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        ResultSet rset = null;
        prepStat = null;
        byte[] testByte = null;
        InputStream is = null;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(200);

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id1);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        // write again in another row

        prepStat = null;
        try
        {

            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id2);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();
            try
            {
                blob.free();
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
            connection.commit();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        // change origin blob

        final byte[] bx = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            bx[i] = (byte) (i + 25);
        }

        bais1 = new ByteArrayInputStream(bx);
        blob = driver.createBlob(connection);
        os = blob.setBinaryStream(1);

        while ((len = bais1.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais1.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("update " + this.table1Name + " set col_blob = ?  where id = ? ");
            driver.setBlob(connection, prepStat, blob, 1);
            prepStat.setString(2, id1);
            prepStat.executeUpdate();
            connection.commit();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        // read again

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id2);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
            is = blob.getBinaryStream();

            while ((len = is.read(buf)) > 0)
            {
                baos.write(buf, 0, len);
            }
            is.close();
            baos.close();
            testByte = baos.toByteArray();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        assertEquals("byte length should be correct", b1.length, testByte.length);
        for (int i = 0; i < b1.length; i++)
        {
            assertEquals("byte should be correct", b1[i], testByte[i]);
        }

        blob.free();
        connection.setAutoCommit(ac);
    }

    @Test
    public void test000705testBlobNull() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b1 = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b1[i] = (byte) (i + 20);
        }

        final String id1 = UUID.randomUUID().toString();

        final ByteArrayInputStream bais1 = new ByteArrayInputStream(b1);

        PreparedStatement prepStat = null;

        Blob blob = driver.createBlob(connection);
        final OutputStream os = blob.setBinaryStream(1);

        final byte[] buf = new byte[27];
        int len;
        while ((len = bais1.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais1.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id1);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();

            connection.commit();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        try
        {
            prepStat = connection.prepareStatement("update  " + this.table1Name + " set col_blob = ? where id = ? ");
            driver.cleanBlob(connection, blob);
            driver.setBlob(connection, prepStat, null, 1);
            prepStat.setString(2, id1);
            prepStat.executeUpdate();
            connection.commit();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        ResultSet rset = null;
        prepStat = null;

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id1);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
            if(blob != null)
            {
                blob.free();
            }
            assertNull("blob schould be null", blob);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        connection.setAutoCommit(ac);
    }

    @Test
    public void test000706testBlobInsertAndReadTwice() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        final TableSpec table1 = spec.addTable(this.table1Name);

        table1.addColumn("id", IColumnType.ColumnType.CHAR.toString(), false, 36);
        table1.addColumn(this.columnBinaryName, IColumnType.ColumnType.BINARY.toString());
        table1.addColumn(this.columnBlobName, IColumnType.ColumnType.BLOB.toString());

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b[i] = (byte) (i + 20);
        }

        final String id = UUID.randomUUID().toString();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);

        PreparedStatement prepStat = null;

        Blob blob = driver.createBlob(connection);
        final OutputStream os = blob.setBinaryStream(1);

        final byte[] buf = new byte[27];
        int len;
        while ((len = bais.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();
            connection.commit();

            bais.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        ResultSet rset = null;
        prepStat = null;
        byte[] testByte = null;
        InputStream is = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(200);

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
            is = blob.getBinaryStream();

            while ((len = is.read(buf)) > 0)
            {
                baos.write(buf, 0, len);
            }
            is.close();
            baos.close();
            testByte = baos.toByteArray();

            baos = new ByteArrayOutputStream(200);
            is = blob.getBinaryStream();

            while ((len = is.read(buf)) > 0)
            {
                baos.write(buf, 0, len);
            }
            is.close();
            baos.close();
            testByte = baos.toByteArray();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        blob.free();

        assertEquals("byte length should be correct", b.length, testByte.length);
        for (int i = 0; i < b.length; i++)
        {
            assertEquals("byte should be correct", b[i], testByte[i]);
        }

        connection.setAutoCommit(ac);
    }

    @Test
    public void test000707testBlobInsertAndWriteReadedWithExc() throws SQLException, ClassNotFoundException, IOException
    {
        if(!this.testConnection.enabled)
        {
            return;
        }
        final Connection connection = this.testConnection.connection;
        final IDatabaseSchemaDriver driver = this.databaseSchemaProcessor.getDatabaseSchemaDriver(connection);

        final SchemaSpec spec = new SchemaSpec(this.databaseID);
        spec.setDbmsSchemaName(this.testConnection.dbmsSchemaName);

        final TableSpec table1 = spec.addTable(this.table1Name);

        table1.addColumn("id", IColumnType.ColumnType.CHAR.toString(), false, 36);
        table1.addColumn(this.columnBinaryName, IColumnType.ColumnType.BINARY.toString());
        table1.addColumn(this.columnBlobName, IColumnType.ColumnType.BLOB.toString());

        this.databaseSchemaProcessor.checkSchemaSpec(spec, connection);

        final boolean ac = connection.getAutoCommit();
        connection.setAutoCommit(false);

        final byte[] b = new byte[200];
        for (int i = 0; i < 200; i++)
        {
            b[i] = (byte) (i + 20);
        }

        final String id = UUID.randomUUID().toString();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);

        PreparedStatement prepStat = null;

        Blob blob = driver.createBlob(connection);
        OutputStream os = blob.setBinaryStream(1);

        final byte[] buf = new byte[27];
        int len;
        while ((len = bais.read(buf)) > 0)
        {
            os.write(buf, 0, len);
        }
        bais.close();
        os.close();

        try
        {
            prepStat = connection.prepareStatement("insert into " + this.table1Name + " (id,col_blob) values (?,?) ");
            prepStat.setString(1, id);
            driver.setBlob(connection, prepStat, blob, 2);
            prepStat.executeUpdate();
            connection.commit();

            bais.close();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            if(prepStat != null)
            {
                prepStat.close();
            }
        }

        blob.free();

        ResultSet rset = null;
        prepStat = null;

        try
        {
            prepStat = connection.prepareStatement("select col_blob from " + this.table1Name + " where id = ? ");
            prepStat.setString(1, id);

            rset = prepStat.executeQuery();
            rset.next();
            blob = driver.getBlob(connection, rset, 1);
            os = blob.setBinaryStream(1);
            os.write("Gehobener Zeigefinger: Das darf man aber nicht!".getBytes());
            os.flush();

            try
            {
                blob.free();
            }
            catch (final Exception e) { }

            try
            {
                connection.setAutoCommit(ac);
            }
            catch (final Exception e) { }
            fail("Expected SQLException to be thrown");
            return;
        }
        catch (final Exception e)
        {
            try
            {
                connection.rollback();
            }
            catch (final Exception e2) { }
            // e.printStackTrace();
        }
        finally
        {
            if(rset != null)
            {
                rset.close();
            }
            if(prepStat != null)
            {
                prepStat.close();
            }

        }

        try
        {
            blob.free();
        }
        catch (final Exception e) { }

        try
        {
            connection.setAutoCommit(ac);
        }
        catch (final Exception e) { }
    }
}
