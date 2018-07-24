[![Build Status](https://travis-ci.org/spalarus/osgi-sodeac-dbschema.svg?branch=master)](https://travis-ci.org/spalarus/osgi-sodeac-dbschema)
# Database Schema Management
An OSGi service inserts and updates database tables, columns and keys. The database and schema must exists. 

## Purpose

Usually relational database objects are managed by heavyweight orm frameworks like hibernate. This project fits, if mapping is unneeded and only a lightweight solution to manage relational database objects is required in OSGi environments.

## Maven

```xml
<dependency>
  <groupId>org.sodeac</groupId>
  <artifactId>org.sodeac.dbschema.api</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>org.sodeac</groupId>
  <artifactId>org.sodeac.dbschema.provider</artifactId>
  <version>1.0.1</version>
</dependency>
```

## Install to local m2-Repository (+ H2 schema driver)

```
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.api:1.0.0"
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.provider:1.0.1"
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.driver.h2:1.0.0"
```

## Install to Apache Karaf / Apache ServiceMix (+ H2 schema driver)

```
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.api/1.0.0
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.provider/1.0.1
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.driver.base/1.0.0
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.driver.h2/1.0.0
```

## OSGi-Dependencies

Bundle-RequiredExecutionEnvironment: JavaSE-1.8

org.osgi.framework;version="[1.8,2)"<br>
org.osgi.service.component;version="[1.3,2)"<br>
org.osgi.service.log;version="[1.3,2)"<br>

## Purpose



## Getting Started

Inject [IDatabaseSchemaProcessor](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IDatabaseSchemaProcessor.html) into your component.

```java
@Reference
protected volatile IDatabaseSchemaProcessor databaseSchemaProcessor = null;
```

### Usage: create simple [schema](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/SchemaSpec.html) with java fluent api

```java
SchemaSpec spec = new SchemaSpec("business");
spec.setDbmsSchemaName(connection.getSchema());

spec.addTable("company")		
.addColumn("id", IColumnType.ColumnType.CHAR.toString(),false,36)
	.setPrimaryKey()
	.endColumnDefinition()
.addColumn("company_name", IColumnType.ColumnType.VARCHAR.toString(),false,256)
	.endColumnDefinition()
.addColumn("established_since", IColumnType.ColumnType.DATE.toString(),true)
	.endColumnDefinition()
;

spec.addTable("employee")	
.addColumn("id", IColumnType.ColumnType.CHAR.toString(),false,36)
	.setPrimaryKey()
	.endColumnDefinition()
.addColumn("company_id", IColumnType.ColumnType.CHAR.toString(),true,36)
	.setForeignKey("fk1_employee", "company","id")
	.endColumnDefinition()
.addColumn("employee_name", IColumnType.ColumnType.VARCHAR.toString(),false,256)
	.endColumnDefinition()
.addColumn("birthday", IColumnType.ColumnType.DATE.toString(),false)
	.endColumnDefinition()
.addColumn("date_of_joining", IColumnType.ColumnType.DATE.toString(),false)
	.endColumnDefinition()
.addColumn("date_of_leaving", IColumnType.ColumnType.DATE.toString(),true)
	.endColumnDefinition()
;

schemaProcessor.checkSchemaSpec(spec, connection);
```

## Supported dbm systems

| DBMS                 | Provider-Bundle                                   | 
|----------------------|---------------------------------------------------|
| H2                   | org.sodeac:org.sodeac.dbschema.driver.h2          |
| PostgreSQL           | org.sodeac:org.sodeac.dbschema.driver.postgresql  |
| Oracle               | org.sodeac:org.sodeac.dbschema.driver.oracle      |

More database managment systems can be supported by providing an OSGi service implements [IDatabaseSchemaDriver](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IDatabaseSchemaDriver.html) .

## Supported column types

| Type                 | Key                                               |  Provider-Bundle                                |
|----------------------|---------------------------------------------------|-------------------------------------------------|
| char                 | IColumnType.ColumnType.CHAR.toString()            | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| varchar              | IColumnType.ColumnType.VARCHAR.toString()         | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| clob                 | IColumnType.ColumnType.CLOB.toString()            | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| boolean              | ColumnType.BOOLEAN.toString()                     | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| smallint             | ColumnType.SMALLINT.toString()                    | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| integer              | ColumnType.INTEGER.toString()                     | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| bigint               | ColumnType.BIGINT.toString()                      | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| real                 | ColumnType.REAL.toString()                        | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| double               | ColumnType.DOUBLE.toString()                      | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| timestamp            | ColumnType.TIMESTAMP.toString()                   | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| date                 | ColumnType.DATE.toString()                        | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| time                 | ColumnType.TIME.toString()                        | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| binary               | ColumnType.BINARY.toString()                      | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |
| blob                 | ColumnType.BLOB.toString()                        | org.sodeac:org.sodeac.dbschema.provider:1.0.1   |

More column types can be supported by providing an OSGi service implements [IColumnType](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IColumnType.html) .

## Limits

* support only single column primary keys
* changes in primary key specification are ignored in already existing database tables
* changes in tablespace specification are ignored in already existing database tables and keys/indices
* changes in column type are limit by limits of used dbms
* removing tables, columns and indices are ignored if objects already exist
* only one foreign key specification is allowed for one tablecolumn
* no support to specify functions, procedures, trigger, sequences and views

## License
[Eclipse Public License 2.0](https://github.com/spalarus/osgi-sodeac-dbschema/blob/master/LICENSE)

