package org.sodeac.dbschema.itest;

import static org.ops4j.pax.exam.CoreOptions.cleanCaches;
import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.MavenArtifactUrlReference;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.sodeac.dbschema.itest.test.util.*;

public interface ITestBase extends Serializable
{
	
	static Map<Integer, ISerializableCallable> connections = new LinkedHashMap<>(); 
	
	public static Map<Integer, ISerializableCallable> connections(Map<String,Boolean> createdSchema)
	{
		
		String schemaname = "S" + TestTools.getSchemaName();
		
		if(connections.isEmpty())
		{
			connections.put(1, new TestConnection1(createdSchema, schemaname));
			connections.put(2, new TestConnection2(createdSchema, schemaname));
			connections.put(3, new TestConnection4(createdSchema, schemaname));
		}
		
		return connections;
	}
	
    public static List<Object[]> testParams()
    {
    	return Arrays.asList(
    		new Object[][] 
    			{
		    		{ 1 },
		
		    		{ 2 },
		
		    		{ 3 }
    			}
    		);
    }
	
	public static Option[] config() 
	{
		MavenArtifactUrlReference karafUrl = maven()
			.groupId("org.apache.karaf")
			.artifactId("apache-karaf")
			.version("4.4.5")
			.type("zip");
	
		MavenUrlReference karafStandardRepo = maven()
			.groupId("org.apache.karaf.features")
			.artifactId("standard")
			.version("4.4.5")
			.classifier("features")
			.type("xml");
	
		MavenUrlReference itestStandardRepo = maven()
			.groupId("org.sodeac")
			.artifactId("org.sodeac.dbschema.itest.features")
			.version("0.0.1-SNAPSHOT")
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
			features(karafStandardRepo , "wrap"),
			features(karafStandardRepo,"jdbc"),
			features(karafStandardRepo,"transaction"),
			features(karafStandardRepo,"jasypt-encryption"),
			features(karafStandardRepo,"jndi"),
			features(karafStandardRepo,"pax-jdbc"),
			features(karafStandardRepo,"pax-jdbc-spec"),
			features(itestStandardRepo,"org.sodeac.dbschema"),
			mavenBundle("org.easymock", "easymock", "3.4"),
//			mavenBundle("com.oracle.database.jdbc", "ojdbc11", "23.3.0.23.09").start(),
//			wrappedBundle(maven("com.oracle.database.jdbc", "ojdbc10", "19.22.0.0")).start(),
			//mavenBundle("mysql", "mysql-connector-java", "6.0.6").start(),
//			Statics.ENABLED_DB2 ? 
//					mavenBundle("org.sodeac", "org.sodeac.thirdparty.jdbcdriver.db2", "2.0.0").start() : 
//					mavenBundle("com.h2database", "h2", "2.2.224"), 
			
//			mavenBundle("org.postgresql", "postgresql", "42.7.2"),
//			mavenBundle("com.h2database", "h2", "2.2.224"),
//			Statics.ENABLED_ORACLE_12 ? mavenBundle("org.sodeac", "org.sodeac.dbschema.driver.oracle", "2.0.0") : mavenBundle("com.h2database", "h2", "2.2.224") ,
//			
//			TestTools.reactorBundle("org.sodeac.dbschema.api","2.0.0"),
//			TestTools.reactorBundle("org.sodeac.dbschema.driver.base","2.0.0"),
//			TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","2.0.0"),
//			TestTools.reactorBundle("org.sodeac.dbschema.driver.postgresql","2.0.0"),
//			//TestTools.reactorBundle("org.sodeac.dbschema.driver.mysql","1.0.0").start(),
//			Statics.ENABLED_ORACLE_12 ? TestTools.reactorBundle("org.sodeac.dbschema.driver.oracle","2.0.0") : TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","2.0.0"),
//			TestTools.reactorBundle("org.sodeac.dbschema.provider","2.0.0"),
//			TestTools.reactorBundle("org.sodeac.dbschema.itest.utils","2.0.0")
					
			//ITestBase.ENABLED_DB2 ? 
			//		TestTools.reactorBundle("org.sodeac.dbschema.driver.db2","1.0.0").start() :
			//		TestTools.reactorBundle("org.sodeac.dbschema.driver.h2","1.0.0").start(),
		};
	}
	
}
