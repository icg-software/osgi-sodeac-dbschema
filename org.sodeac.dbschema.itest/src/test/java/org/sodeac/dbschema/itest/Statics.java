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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.karaf.options.LogLevelOption.LogLevel;
import org.ops4j.pax.exam.options.MavenArtifactUrlReference;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.sodeac.dbschema.itest.testconnections.DB2TestConnectionFactory;
import org.sodeac.dbschema.itest.testconnections.EDbType;
import org.sodeac.dbschema.itest.testconnections.H2TestConnectionFactory;
import org.sodeac.dbschema.itest.testconnections.MySqlTestConnectionFactory;
import org.sodeac.dbschema.itest.testconnections.Oracle12TestConnectionFactory;
import org.sodeac.dbschema.itest.testconnections.PostgresTestConnectionFactory;

import lombok.val;

public class Statics
{
    public static final Boolean ENABLED_H2 = true;
    public static final Boolean ENABLED_POSTGRES = false;
    public static final Boolean ENABLED_ORACLE_12 = false;

    public static final Boolean ENABLED_DB2 = false;
    public static final Boolean ENABLED_MYSQL = false;

    // TODO mvn -DmyVariable=someValue for DB Config

    public static List<Object[]> connections(final Map<String, Boolean> createdSchema)
    {
        final String schemaName = "S" + TestTools.getSchemaName();
        return Arrays.asList(new Object[][]
                {
                        { new H2TestConnectionFactory(createdSchema, schemaName) },
                        { new PostgresTestConnectionFactory(createdSchema, schemaName) },
                        { new MySqlTestConnectionFactory(createdSchema, schemaName) },
                        { new Oracle12TestConnectionFactory(createdSchema, schemaName) },
                        { new DB2TestConnectionFactory(createdSchema, schemaName) }

                }
        );
    }

    public static Option[] config()
    {
        final MavenArtifactUrlReference karafUrl = maven()
                .groupId("org.apache.karaf")
                .artifactId("apache-karaf")
                .versionAsInProject()
                .type("zip");

        final MavenUrlReference karafStandardRepo = maven()
                .groupId("org.apache.karaf.features")
                .artifactId("standard")
                .versionAsInProject()
                .classifier("features")
                .type("xml");

        val easymock = mavenBundle("org.easymock", "easymock").versionAsInProject();
        val postgresql = mavenBundle("org.postgresql", "postgresql").versionAsInProject();
        val h2 = mavenBundle("com.h2database", "h2").versionAsInProject();
        val sodeacVersion = System.getProperty("sodeac.version", "2.0.0-SNAPSHOT");

        System.out.println("########################################################################################");
        System.out.println(karafUrl);
        System.out.println(karafStandardRepo);
        System.out.println(easymock);
        System.out.println(postgresql);
        System.out.println(h2);
        System.out.println(sodeacVersion);
        return new Option[]
                {
                        karafDistributionConfiguration()
                                .frameworkUrl(karafUrl)
                                .unpackDirectory(new File("target", "exam"))
                                .useDeployFolder(false),
                        keepRuntimeFolder(),
                        cleanCaches(true),
                        logLevel(LogLevel.INFO),
                        features(karafStandardRepo, "scr"),
                        features(karafStandardRepo, "jdbc"),
                        features(karafStandardRepo, "transaction"),
                        features(karafStandardRepo, "jasypt-encryption"),
                        features(karafStandardRepo, "jndi"),
                        features(karafStandardRepo, "pax-jdbc"),
                        features(karafStandardRepo, "pax-jdbc-spec"),
                        easymock.start(),
                        postgresql.start(),
                        h2.start(),
                        // mavenBundle("mysql", "mysql-connector-java", "6.0.6").start(),
                        Statics.ENABLED_DB2 ?
                                mavenBundle("org.sodeac", "org.sodeac.thirdparty.jdbcdriver.db2", "1.0.0").start() :
                                h2,
                        Statics.ENABLED_ORACLE_12 ?
                                mavenBundle("org.sodeac", "org.sodeac.thirdparty.jdbcdriver.oracle", "1.0.0").start() :
                                h2
                        ,

                        TestTools.reactorBundle("org.sodeac.dbschema.api", sodeacVersion).start(),
                        TestTools.reactorBundle("org.sodeac.dbschema.driver.base", sodeacVersion).start(),
                        TestTools.reactorBundle("org.sodeac.dbschema.driver.h2", sodeacVersion).start(),
                        TestTools.reactorBundle("org.sodeac.dbschema.driver.postgresql", sodeacVersion).start(),
                        // TestTools.reactorBundle("org.sodeac.dbschema.driver.mysql",sodeacVersion).start(),
                        Statics.ENABLED_ORACLE_12 ?
                                TestTools.reactorBundle("org.sodeac.dbschema.driver.oracle", sodeacVersion).start() :
                                TestTools.reactorBundle("org.sodeac.dbschema.driver.h2", sodeacVersion).start(),
                        // Statics.ENABLED_DB2 ?
                        //		TestTools.reactorBundle("org.sodeac.dbschema.driver.db2",sodeacVersion).start() :
                        //		TestTools.reactorBundle("org.sodeac.dbschema.driver.h2",sodeacVersion).start(),
                        TestTools.reactorBundle("org.sodeac.dbschema.provider", sodeacVersion).start()
                };
    }

    public static TestConnection createConnection(final EDbType dbType, final Map<String, Boolean> createdSchema) throws SQLException, ClassNotFoundException
    {
        System.out.println("##################################");
        System.out.println(createdSchema);
        final String schemaName = "S" + TestTools.getSchemaName();
        return switch (dbType)
        {
            case H2 -> new H2TestConnectionFactory(createdSchema, schemaName).call();
            case POSTGRES -> new PostgresTestConnectionFactory(createdSchema, schemaName).call();
            case MYSQL -> new MySqlTestConnectionFactory(createdSchema, schemaName).call();
            case ORACLE_12 -> new Oracle12TestConnectionFactory(createdSchema, schemaName).call();
            case DB2 -> new DB2TestConnectionFactory(createdSchema, schemaName).call();
        };
    }
}
