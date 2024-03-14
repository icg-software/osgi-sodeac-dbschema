package org.sodeac.dbschema.itest.test.util;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

public class TestConnection2 implements ISerializableCallable
{
	
	private static final long serialVersionUID = 1L;
	
	private final Map<String,Boolean> createdSchema;
	private final String schemaName;
//	private final String schemaName = "S" + TestTools.getSchemaName();
	
	public TestConnection2(Map<String,Boolean> createdSchema, String schemaName)
	{
		super();
		this.createdSchema = createdSchema;
		this.schemaName = schemaName;
	}


	@Override
	public TestConnection call() throws Exception
	{
		TestConnection testConnection = new TestConnection(Statics.ENABLED_POSTGRES);
		if(! testConnection.enabled)
		{
			return testConnection;
		}
		
		// docker run --name postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sodeac -d -p 5432:5432 postgres
		
		// docker exec -it postgres bash:
		// mkdir /var/lib/postgresql/data/sodeacdata
		// mkdir /var/lib/postgresql/data/sodeacindex
		// chown postgres.postgres /var/lib/postgresql/data/sodeacdata
		// chown postgres.postgres /var/lib/postgresql/data/sodeacindex
		
		// psql -h 127.0.0.1 -U postgres --dbname=postgres ::
		
		// CREATE USER sodeac with SUPERUSER CREATEDB CREATEROLE INHERIT REPLICATION LOGIN PASSWORD 'sodeac';
		// CREATE TABLESPACE sodeacdata OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacdata';
		// CREATE TABLESPACE sodeacindex OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacindex';
		// 
		
		
		// CREATE SCHEMA IF NOT EXISTS sodeac1 AUTHORIZATION sodeac;
		try
		{
			Class.forName("org.postgresql.Driver").getConstructor().newInstance();
		}
		catch (Exception e) {}
		testConnection.connection = DriverManager.getConnection("jdbc:postgresql://192.168.178.45:5432/sodeac", "sodeac", "sodeac");
		
		if(createdSchema.get("POSTGRES_" + schemaName) == null)
		{
			createdSchema.put("POSTGRES_" + schemaName,true);
			
			PreparedStatement prepStat = testConnection.connection.prepareStatement("CREATE SCHEMA IF NOT EXISTS " + schemaName.toLowerCase() + " AUTHORIZATION sodeac");
			prepStat.executeUpdate();
			prepStat.close();
		}
		testConnection.connection.setSchema(schemaName.toLowerCase());
		testConnection.dbmsSchemaName = schemaName.toLowerCase();
		
		return testConnection;
	}
	
}
