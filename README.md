[![Build Status](https://travis-ci.org/spalarus/osgi-sodeac-dbschema.svg?branch=master)](https://travis-ci.org/spalarus/osgi-sodeac-dbschema)

# Database Schema Management

An OSGi service inserts and updates database tables, columns and keys. The database and schema must exists.

## Installation

- runs with Apache Karaf 4.4.10 -> OSGi 8.0.0
- `mvn clean install`

### Karaf

#### Debugging

```bash
# open karaf console in debug mode
karaf debug
```

```bash
# --- project bundle
install -s mvn:org.sodeac/org.sodeac.dbschema.api/2.0.0-SNAPSHOT
install -s mvn:org.sodeac/org.sodeac.dbschema.provider/2.0.0-SNAPSHOT
install -s mvn:org.sodeac/org.sodeac.dbschema.driver.base/2.0.0-SNAPSHOT
install -s mvn:org.sodeac/org.sodeac.dbschema.driver.h2/2.0.0-SNAPSHOT
install -s mvn:org.sodeac/org.sodeac.dbschema.driver.postgresql/2.0.0-SNAPSHOT
install -s mvn:org.sodeac/org.sodeac.dbschema.driver.oracle/2.0.0-SNAPSHOT

bundle:watch org.sodeac.dbschema.api org.sodeac.dbschema.provider org.sodeac.dbschema.driver.base org.sodeac.dbschema.driver.h2 org.sodeac.dbschema.driver.postgresql org.sodeac.dbschema.driver.oracle ```

## Purpose

Usually relational database schema is managed by heavyweight orm frameworks like hibernate. DBSchema is an alternative, if mapping is unneeded and only a lightweight solution to manage relational database objects is required in OSGi
environments.

## Maven

```xml

<dependency>
    <groupId>org.sodeac</groupId>
    <artifactId>org.sodeac.dbschema.api</artifactId>
    <version>2.0.0-SNAPSHOT</version>
</dependency>
<dependency>
<groupId>org.sodeac</groupId>
<artifactId>org.sodeac.dbschema.provider</artifactId>
<version>2.0.0-SNAPSHOT</version>
</dependency>
```

## Install to local m2-Repository (+ H2 schema driver)

```
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.api:2.0.0-SNAPSHOT"
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT"
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get -Dartifact="org.sodeac:org.sodeac.dbschema.driver.h2:2.0.0-SNAPSHOT"
```

## Install to Apache Karaf / Apache ServiceMix (+ H2 schema driver)

```
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.api/2.0.0-SNAPSHOT
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.provider/2.0.0-SNAPSHOT
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.driver.base/2.0.0-SNAPSHOT
bundle:install -s mvn:org.sodeac/org.sodeac.dbschema.driver.h2/2.0.0-SNAPSHOT
```

## OSGi-Dependencies

- Java 21
- OSGi 8.0.0

## Purpose

## Getting Started

Inject [IDatabaseSchemaProcessor](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IDatabaseSchemaProcessor.html)
into your component.

```java

@Reference
private final IDatabaseSchemaProcessor databaseSchemaProcessor = null;
```

### Usage: create simple [schema](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/SchemaSpec.html) with java fluent api

```java
SchemaSpec spec = new SchemaSpec("business");
spec.

setDbmsSchemaName(connection.getSchema());

        spec.

addTable("company")		
.

addColumn("id",IColumnType.ColumnType.CHAR.toString(),false,36)
        .

setPrimaryKey()
	.

endColumnDefinition()
.

addColumn("company_name",IColumnType.ColumnType.VARCHAR.toString(),false,256)
        .

endColumnDefinition()
.

addColumn("established_since",IColumnType.ColumnType.DATE.toString(),true)
        .

endColumnDefinition()
        ;

spec.

addTable("employee")	
.

addColumn("id",IColumnType.ColumnType.CHAR.toString(),false,36)
        .

setPrimaryKey()
	.

endColumnDefinition()
.

addColumn("company_id",IColumnType.ColumnType.CHAR.toString(),true,36)
        .

setForeignKey("fk1_employee","company","id")
	.

endColumnDefinition()
.

addColumn("employee_name",IColumnType.ColumnType.VARCHAR.toString(),false,256)
        .

endColumnDefinition()
.

addColumn("birthday",IColumnType.ColumnType.DATE.toString(),false)
        .

endColumnDefinition()
.

addColumn("date_of_joining",IColumnType.ColumnType.DATE.toString(),false)
        .

endColumnDefinition()
.

addColumn("date_of_leaving",IColumnType.ColumnType.DATE.toString(),true)
        .

endColumnDefinition()
        ;

schemaProcessor.

checkSchemaSpec(spec, connection);
```

## Supported dbm systems

| DBMS       | Provider-Bundle                                  | 
|------------|--------------------------------------------------|
| H2         | org.sodeac:org.sodeac.dbschema.driver.h2         |
| PostgreSQL | org.sodeac:org.sodeac.dbschema.driver.postgresql |
| Oracle     | org.sodeac:org.sodeac.dbschema.driver.oracle     |

More database managment systems can be supported by providing an OSGi service
implements [IDatabaseSchemaDriver](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IDatabaseSchemaDriver.html) .

## Supported column types

| Type      | Key                                       | Provider-Bundle                                        |
|-----------|-------------------------------------------|--------------------------------------------------------|
| char      | IColumnType.ColumnType.CHAR.toString()    | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| varchar   | IColumnType.ColumnType.VARCHAR.toString() | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| clob      | IColumnType.ColumnType.CLOB.toString()    | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| boolean   | ColumnType.BOOLEAN.toString()             | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| smallint  | ColumnType.SMALLINT.toString()            | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| integer   | ColumnType.INTEGER.toString()             | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| bigint    | ColumnType.BIGINT.toString()              | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| real      | ColumnType.REAL.toString()                | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| double    | ColumnType.DOUBLE.toString()              | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| timestamp | ColumnType.TIMESTAMP.toString()           | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| date      | ColumnType.DATE.toString()                | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| time      | ColumnType.TIME.toString()                | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| binary    | ColumnType.BINARY.toString()              | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |
| blob      | ColumnType.BLOB.toString()                | org.sodeac:org.sodeac.dbschema.provider:2.0.0-SNAPSHOT |

More column types can be supported by providing an OSGi service
implements [IColumnType](https://oss.sonatype.org/service/local/repositories/releases/archive/org/sodeac/org.sodeac.dbschema.api/1.0.0/org.sodeac.dbschema.api-1.0.0-javadoc.jar/!/org/sodeac/dbschema/api/IColumnType.html) .

## Limits

* only single column primary keys are supported
* primary key specification updates are ignored in already existing database tables
* tablespaces specification updates are ignored in already existing database tables or keys/indices
* column types updates are limited by limits of used dbms
* removing tables, columns and indices from schema specification are ignored if objects already exist in dbms
* only one foreign key specification is allowed for one tablecolumn
* no support to specify functions, procedures, trigger, sequences and views

## License

[Eclipse Public License 2.0](https://github.com/spalarus/osgi-sodeac-dbschema/blob/master/LICENSE)

