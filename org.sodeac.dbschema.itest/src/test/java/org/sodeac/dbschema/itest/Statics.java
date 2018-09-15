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
package org.sodeac.dbschema.itest;

import static org.ops4j.pax.exam.CoreOptions.cleanCaches;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.MavenArtifactUrlReference;
import org.ops4j.pax.exam.options.MavenUrlReference;

public class Statics
{
	public static final Boolean ENABLED_H2 = true;
	public static final Boolean ENABLED_POSTGRES = false;
	public static final Boolean ENABLED_ORACLE_12 = false;
	
	public static final Boolean ENABLED_DB2 = false;
	public static final Boolean ENABLED_MYSQL = false;
	
	public static List<Object[]> connections(Map<String,Boolean> createdSchema)
    {
		final String schemaName = "S" + TestTools.getSchemaName();
    	return Arrays.asList
		(
			new Object[][]
			{
				{new Callable<TestConnection>()
				{

					@Override
					public TestConnection call() throws ClassNotFoundException, SQLException
					{
						TestConnection testConnection = new TestConnection(Statics.ENABLED_H2);
						if(! testConnection.enabled)
						{
							return testConnection;
						}
						
						try
						{
							Class.forName("org.h2.Driver").newInstance();
						}
						catch (Exception e) {}
						testConnection.connection = DriverManager.getConnection("jdbc:h2:./../../../test", "sa", "sa");
						
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
				}}
				,
				{new Callable<TestConnection>()
				{

					@Override
					public TestConnection call() throws ClassNotFoundException, SQLException
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
						
						// CREATE USER sodeac with SUPERUSER CREATEDB CREATEROLE INHERIT REPLICATION LOGIN PASSWORD 'sodeac';
						// CREATE TABLESPACE sodeacdata OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacdata';
						// CREATE TABLESPACE sodeacindex OWNER sodeac LOCATION '//var//lib//postgresql//data//sodeacindex';
						// 
						
						
						// CREATE SCHEMA IF NOT EXISTS sodeac1 AUTHORIZATION sodeac;
						try
						{
							Class.forName("org.postgresql.Driver").newInstance();
						}
						catch (Exception e) {}
						testConnection.connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/sodeac", "sodeac", "sodeac");
						
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
				}}
				,
				{new Callable<TestConnection>()
				{

					@Override
					public TestConnection call() throws ClassNotFoundException, SQLException
					{
						TestConnection testConnection = new TestConnection(Statics.ENABLED_MYSQL);
						if(! testConnection.enabled)
						{
							return testConnection;
						}
						
						// docker run --name=mysql -e MYSQL_ROOT_HOST=% -e MYSQL_ROOT_PASSWORD=sodeac -p 3306:3306 -d mysql/mysql-server
						// 
						// CREATE TABLESPACE sodeacdata ADD DATAFILE 'sodeacdata.ibd' ENGINE=INNODB
						// CREATE TABLESPACE sodeacindex ADD DATAFILE 'sodeacindex.ibd' ENGINE=INNODB
						
						try
						{
							Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
						}
						catch (Exception e) {}
						testConnection.connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1/?useSSL=false", "root", "sodeac");
						
						if(createdSchema.get("MYSQL_" + schemaName) == null)
						{
							createdSchema.put("MYSQL_" + schemaName,true);
							
							PreparedStatement prepStat = testConnection.connection.prepareStatement("CREATE SCHEMA " + schemaName.toLowerCase() + " CHARACTER SET = utf8 COLLATE = utf8_general_ci");
							prepStat.executeUpdate();
							prepStat.close();
						}
						testConnection.connection.setSchema(schemaName.toLowerCase());
						testConnection.connection.setCatalog(schemaName.toLowerCase());
						testConnection.dbmsSchemaName = schemaName.toLowerCase();
						
						return testConnection;
					}
					
				}}
				,
				{new Callable<TestConnection>()
				{

					@Override
					public TestConnection call() throws ClassNotFoundException, SQLException
					{
						TestConnection testConnection = new TestConnection(Statics.ENABLED_ORACLE_12);
						if(! testConnection.enabled)
						{
							return testConnection;
						}
						
						// docker run --name oracle -d -p 28080:8080 -p 1521:1521 sath89/oracle-12c
						
						/*
						 * http://localhost:8080/apex
							workspace: INTERNAL
							user: ADMIN
							password: 0Racle$
							
							Apex upgrade up to v 5.*
							
							hostname: localhost
							port: 1521
							sid: xe
							service name: xe
							username: system
							password: oracle
							
						 */
						
						// CREATE TABLESPACE sodeacdata  DATAFILE 'sodeacdata.dbf' SIZE 40M AUTOEXTEND ON NEXT 10M ONLINE;
						// CREATE TABLESPACE sodeacindex  DATAFILE 'sodeacindex.dbf' SIZE 40M AUTOEXTEND ON NEXT 10M ONLINE;
						
						// CREATE USER SODEAC IDENTIFIED BY sodeac DEFAULT TABLESPACE USERS PROFILE DEFAULT
						// GRANT CONNECT TO SODEAC  WITH ADMIN OPTION
						// GRANT RESOURCE TO SODEAC  WITH ADMIN OPTION
						// GRANT DBA TO SODEAC  WITH ADMIN OPTION
						
						/*   select 
								'drop user '||username||' cascade;'
							from dba_users  WHERE username LIKE 'S00%'
						*/
						
						
						try
						{
							Class.forName("oracle.jdbc.OracleDriver").newInstance();
						}
						catch (Exception e) {}
						
						
						if(createdSchema.get("ORACLE_" + schemaName) == null)
						{
							createdSchema.put("ORACLE_" + schemaName,true);
							
							Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521/xe", "system", "oracle");
							
							PreparedStatement prepStat = connection.prepareStatement("CREATE USER " + schemaName.toUpperCase() + " IDENTIFIED BY sodeac DEFAULT TABLESPACE USERS PROFILE DEFAULT");
							prepStat.executeUpdate();
							prepStat.close();
							
							prepStat = connection.prepareStatement("GRANT CONNECT TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
							prepStat.executeUpdate();
							prepStat.close();
							
							prepStat = connection.prepareStatement("GRANT RESOURCE TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
							prepStat.executeUpdate();
							prepStat.close();
							
							prepStat = connection.prepareStatement("GRANT DBA TO " + schemaName.toUpperCase() + "  WITH ADMIN OPTION");
							prepStat.executeUpdate();
							prepStat.close();
							
							connection.close();
						}
						testConnection.connection = DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521/xe", schemaName.toUpperCase(), "sodeac");
						testConnection.connection.setSchema(schemaName.toUpperCase());
						testConnection.dbmsSchemaName = schemaName.toUpperCase();
						
						return testConnection;
					}
					
				}}
				,{new Callable<TestConnection>()
				{

					@Override
					public TestConnection call() throws ClassNotFoundException, SQLException
					{
						TestConnection testConnection = new TestConnection(Statics.ENABLED_DB2);
						if(! testConnection.enabled)
						{
							return testConnection;
						}
						
						// docker run --name db2 -d -p 50000:50000 -e DB2INST1_PASSWORD=sodeac -e LICENSE=accept  ibmcom/db2express-c:latest db2start
						// docker exec -it db2 bash:
						// su db2inst1
						// db2 create db sodeac
						
						// CREATE TABLESPACE SODEACDATA MANAGED BY AUTOMATIC STORAGE
						// CREATE TABLESPACE SODEACINDEX MANAGED BY AUTOMATIC STORAGE
						
						try
						{
							Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance().getClass();
						}
						catch (Exception e) {}
						
						testConnection.connection = DriverManager.getConnection("jdbc:db2://127.0.0.1:50000/sodeac", "db2inst1", "sodeac");
						
						if(createdSchema.get("DB2_" + schemaName) == null)
						{
							createdSchema.put("DB2_" + schemaName,true);
							
							PreparedStatement prepStat = testConnection.connection.prepareStatement("CREATE SCHEMA " + schemaName.toUpperCase() + " AUTHORIZATION DB2INST1");
							prepStat.executeUpdate();
							prepStat.close();
						}
						testConnection.connection.setSchema(schemaName.toUpperCase());
						testConnection.dbmsSchemaName = schemaName.toUpperCase();
						
						return testConnection;
						
						
						
					}
					
				}}			
				
			}
		);
    }
	
	public static Option[] config() 
	{
		MavenArtifactUrlReference karafUrl = maven()
			.groupId("org.apache.karaf")
			.artifactId("apache-karaf")
			.version("4.1.6")
			.type("zip");

		MavenUrlReference karafStandardRepo = maven()
			.groupId("org.apache.karaf.features")
			.artifactId("standard")
			.version("4.1.6")
			.classifier("features")
			.type("xml");
		
		return new Option[] 
		{
			karafDistributionConfiguration()
				.frameworkUrl(karafUrl)
				.unpackDirectory(new File("target", "exam"))
				.useDeployFolder(false),
			keepRuntimeFolder(),
			cleanCaches( true ),
			logLevel(LogLevel.INFO),
			features(karafStandardRepo , "scr"),
			features(karafStandardRepo,"jdbc"),
			features(karafStandardRepo,"transaction"),
			features(karafStandardRepo,"jasypt-encryption"),
			features(karafStandardRepo,"jndi"),
			features(karafStandardRepo,"pax-jdbc"),
			features(karafStandardRepo,"pax-jdbc-spec"),
			mavenBundle("org.easymock", "easymock", "3.4").start(),
			mavenBundle("org.postgresql", "postgresql", "42.2.2").start(),
			mavenBundle("com.h2database", "h2", "1.4.197").start(),
			//mavenBundle("mysql", "mysql-connector-java", "6.0.6").start(),
			Statics.ENABLED_DB2 ? 
					mavenBundle("org.sodeac", "org.sodeac.thirdparty.jdbcdriver.db2", "1.0.0").start() : 
					mavenBundle("com.h2database", "h2", "1.4.197"), 
			Statics.ENABLED_ORACLE_12 ? 
					mavenBundle("org.sodeac", "org.sodeac.thirdparty.jdbcdriver.oracle", "1.0.0").start() :
					mavenBundle("com.h2database", "h2", "1.4.197")
			,
			
			TestTools.reactorBundle("org.sodeac.dbschema.api","1.0.0").start(),
			TestTools.reactorBundle("org.sodeac.dbschema.driver.base","1.0.0").start(),
			TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","1.0.0").start(),
			TestTools.reactorBundle("org.sodeac.dbschema.driver.postgresql","1.0.0").start(),
			//TestTools.reactorBundle("org.sodeac.dbschema.driver.mysql","1.0.0").start(),
			Statics.ENABLED_ORACLE_12 ?
					TestTools.reactorBundle("org.sodeac.dbschema.driver.oracle","1.0.0").start() :
					TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","1.0.0").start(),
			//Statics.ENABLED_DB2 ? 
			//		TestTools.reactorBundle("org.sodeac.dbschema.driver.db2","1.0.0").start() :
			//		TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","1.0.0").start(),
			TestTools.reactorBundle("org.sodeac.dbschema.provider","1.0.1").start()
		};
	}
}
