package org.sodeac.dbschema.itest.test.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

public class TestConnection1 implements ISerializableCallable
{
	
	private static final long serialVersionUID = 1L;
	
	private final Map<String,Boolean> createdSchema;
	private final String schemaName;
//	private final String schemaName = "S" + TestTools.getSchemaName();
	
	public TestConnection1(Map<String,Boolean> createdSchema, String schemaName)
	{
		super();
		this.createdSchema = createdSchema;
		this.schemaName = schemaName;
	}

	@Override
	public TestConnection call() throws Exception
	{
		TestConnection testConnection = new TestConnection(Statics.ENABLED_H2);
		if(! testConnection.enabled)
		{
			return testConnection;
		}
		
		try
		{
			Class.forName("org.h2.Driver").getConstructor().newInstance();
		}
		catch (Exception e) {}
		
		Path h2Path = Paths.get("./../../../test");
		
		if(Files.exists(h2Path))
		{
			Files.delete(h2Path);
		}
		
		testConnection.connection = DriverManager.getConnection(String.format("jdbc:h2:%s", h2Path.toString()), "sa", "sa");
		
		if(createdSchema.get("H2_" + schemaName) == null)
		{
			createdSchema.put("H2_" + schemaName,true);
			
			PreparedStatement prepStat = testConnection.connection.prepareStatement("CREATE SCHEMA " + schemaName );
			prepStat.executeUpdate();
			prepStat.close();
		}
		testConnection.connection.setSchema(schemaName);
		testConnection.dbmsSchemaName = schemaName;
		return testConnection;
	}
	
}
