# How to add a driver to VerdictDB for support of your own database 

This user guide explains steps required to add a driver for your database into VerdictDB.

## Prerequisites
1. A JDBC driver should be available for the database that you want to add a driver for.
1. The database must support functions that VerdictDB requires for its operations (e.g., *rand()*). This will be explained later in the guide.

## Overview
In a nutshell, a user needs to do the following to add a driver into VerdictDB:

1. Fork the VerdictDB's public repository.
2. Add a VerdictDB driver class for your database and implement appropriate methods.
3. Make the driver available by modifying the if-else-if statements for the driver instantiation in VerdictDB.
4. Add properties in VerdictDB's default property file.
5. Add a maven dependency for JDBC driver of your database.
6. Test and submit a pull request for your new driver to the VerdictDB's public repository.

Each step will be explained with detailed instructions below.

## Instructions with Examples

### 1. Fork the VerdictDB's public repository

To add your own driver for VerdictDB, you must create a copy of VerdictDB's public repository by forking it.
You can do this by visiting [VerdictDB's public repository](https://github.com/mozafari/verdict) and clicking **Fork** button at the top-right. 
The detailed instructions on how to fork a repository can be found [here](https://help.github.com/articles/fork-a-repo/). You need to add a driver into your forked repository and make a pull request to the public repository.

### 2. Add a VerdictDB driver for your database and implement appropriate methods

Here we will use VerdictDB's Hive driver as an example. 

Let's suppose we are trying to add a Hive driver for VerdictDB.
A driver for each DBMS that VerdictDB supports is included under the package: **edu.umich.verdict.dbms**. The current implementation of the Hive driver is [here](https://github.com/mozafari/verdict/blob/master/core/src/main/java/edu/umich/verdict/dbms/DbmsHive.java).

The package is located at: *\<verdict_root\>/core/src/main/java/edu/umich/verdict/dbms*

Basically, a VerdictDB's driver to a specific database is a Java class that extends **DbmsJDBC** class, which again inherits **Dbms** class. Since **Dbms** class contains a number of abstract methods that are not implemented by its **DbmsJDBC** subclass, you should add implementations for those abstract methods in your driver. 

Note that it may be necessary to also override some of the methods already implemented in either **Dbms** or **DbmsJDBC** classes if your database does not work with the default behavior defined in the two superclasses.

#### 2.1. Create a driver class

Let's create a java class file *Dbms\<db_name\>*.java under the package **edu.umich.verdict.dbms** (i.e., *DbmsHive.java* in this example) . The class should look something like this:

```java
package edu.umich.verdict.dbms;

public class DbmsHive extends DbmsJDBC {
	/// member methods...
}
```

#### 2.2. Implement necessary abstract methods

Currently there are five abstract methods that you must provide an implementation. Here we explain each of them with the current implementation of VerdictDB's Hive driver as an example.

##### a) protected String randomPartitionColumn()

This method returns a field expression that will go into a SELECT statement, which should give a random partition number for each row.

The current implementation for Hive is the following:

```java
@Override
protected String randomPartitionColumn() {
    int pcount = partitionCount();
    return String.format("pmod(round(rand(unix_timestamp())*%d), %d) AS %s", 
            pcount, pcount, partitionColumnName());
}
```

Using the total number of partitions that can be obtained by calling a private method *partitionCount()* (say *pcount*) and the partition column name from another private method *partitionColumnName()* (say *pName*), the method should return a field expression that will go into a SELECT statement that is equivalent to: "random_number modulo *pcount* as *pName*".

Note that the current implementation uses the following Hive functions:

1. pmod() : the modulo operation
2. round() : conversion from double to integer
3. rand() : random number generation
4. unix_timestamp() : current timestamp as a seed for rand()

Since every database differs not only in which functions it supports, but also in how it supports them, you need to use appropriate functions that are available from your target database.

##### b) protected String randomNumberExpression(SampleParam param)

This method returns a field expression that will go into a SELECT statement, which should give a random number between 0 and 1.

```java
@Override
protected String randomNumberExpression(SampleParam param) {
    String expr = "rand(unix_timestamp())";
    return expr;
}
```

The current implementation of the method for Hive is straightforward with its use of *rand()* function. The *param* argument contains information for creating a sample table, but it is not required to use this information in this method.

##### c) public String modOfHash(String col, int mod)
This method returns a field expression that will go into a SELECT statement. The expression should get a hash value by applying a hash function to the column named **col**, then perform a modulo operation on the hash value with **mod** as a divisor.

The current implementation for Hive is as follows:

```java
@Override
public String modOfHash(String col, int mod) {
    return String.format("crc32(cast(%s%s%s as string)) %% %d", 
        getQuoteString(), col, getQuoteString(), mod);
}
```

For Hive, the current implementation first casts the **col** column as string with *cast()* function, then calculates its hash value by applying *crc32()* function, which is a hash function available in Hive, to the string. The modulo operation is applied at the end with **mod** as a divisor.

##### d) public String modOfHash(List\<String\> columns, int mod)

This method returns a field expression that will go into a SELECT statement. The expression should get a hash value by applying a hash function to all columns listed in **columns**, then perform a modulo operation on the hash value with **mod** as a divisor.

The current implementation of the method for Hive is as follows:

```java
@Override
public String modOfHash(List<String> columns, int mod) {
    String concatStr = "";
    for (int i = 0; i < columns.size(); ++i) {
        String col = columns.get(i);
        String castStr = String.format("cast(%s%s%s as string)", 
            getQuoteString(), col, getQuoteString());
        if (i < columns.size() - 1) {
            castStr += ",";
        }
        concatStr += castStr;
    }
    return String.format("crc32(concat_ws('%s', %s)) %% %d", 
        HASH_DELIM, concatStr, mod);
}
```

This method is similar to the previous **modOfHash(String col, int mod)**. The only difference between this method and the previous method in the current implementation is that here we concatenate casted strings from all columns listed in **columns** with *concat_ws()* function before calculating its hash value.

##### e) protected String modOfRand(int mod)

This method returns a field expression that will go into a SELECT statement, which should give a random number modulo **mod**.

The current implementation for Hive is as follows:

```java
@Override
protected String modOfRand(int mod) {
    return String.format("abs(rand(unix_timestamp())) %% %d", mod);
}
```

This method is similar to *randomNumberExpression()* method. You only need to apply an additional modulo operation with **mod**.

Note that VerdictDB utilizes the functions provided by databases (e.g., *rand(), crc32(), etc.*). It is likely that VerdictDB will not be able to support your database if the database does not provide such functions. If this is the case, please contact us at <verdict-user@umich.edu> to discuss any possible workarounds.

#### 2.3. Override existing methods (only if necessary)

Your database may not support full SQL syntax and thus, the default methods implemented in VerdictDB might not work with your database. In such cases, you may need to override such methods in the **Dbms** or **DbmsJDBC** classes.

For example, the method, *public void insertEntry(TableUniqueName tableName, List\<Object\> values)*, inserts *values* into the table using the standard SQL INSERT statement: *INSERT INTO TABLE \<tableName\> WITH VALUES (...)*

However, the current implementation for Hive overrides this method as follows:

```java
@Override
public void insertEntry(TableUniqueName tableName, List<Object> values) 
        throws VerdictException {
    StringBuilder sql = new StringBuilder(1000);
    sql.append(String.format("insert into table %s select * from 
        (select ", tableName));
    String with = "'";
    sql.append(Joiner.on(", ").join(StringManipulations.quoteString(
        values, with)));
    sql.append(") s");
    executeUpdate(sql.toString());
}
```

The problem here is that some SQL-on-hadoop engines do not support such standard INSERT statement (e.g., older versions of Hive). Therefore, we overrode the method as shown above for the VerdictDB's Hive driver.

We recommend to implement necessary abstract methods first, then override existing methods if there is any problem with default methods during the test.

### 3. Make the driver available in VerdictDB

In the *getInstance()* method under the **Dbms** class, there are if-else-if statements for instantiating a correct driver for the given database:

```java
protected static Dbms getInstance(VerdictContext vc, String dbName, 
        String host, String port, String schema,
        String user, String password, String jdbcClassName) 
        throws VerdictException {

    Dbms dbms = null;

    if (dbName.equals("impala")) {
        dbms = new DbmsImpala(vc, dbName, host, port, schema, user, 
            password, jdbcClassName);
    } else if (dbName.equals("hive") || dbName.equals("hive2")) {
        dbms = new DbmsHive(vc, dbName, host, port, schema, user, 
            password, jdbcClassName);
    } else if (dbName.equals("redshift")) {
        dbms = new DbmsRedshift(vc, dbName, host, port, schema, user, 
            password, jdbcClassName);
    } else if (dbName.equals("dummy")) {
        dbms = new DbmsDummy(vc);
    } else {
        String msg = String.format("Unsupported DBMS: %s", dbName);
        VerdictLogger.error("Dbms", msg);
        throw new VerdictException(msg);
    }

    return dbms;
}
```

We can see that it instantiates the Hive driver with the following else-if statement:

```java
} else if (dbName.equals("hive") || dbName.equals("hive2")) {
    dbms = new DbmsHive(vc, dbName, host, port, schema, user, 
        password, jdbcClassName);
}
```

You simply need to add a similar else-if statement here for your driver.

Note that *dbName* directly goes into its JDBC connection string (e.g., *jdbc:\<dbName\>://\<host\>:\<port\>/\<schema\>*).

### 4. Add properties in the VerdictDB's default property file

You need to add two properties in the VerdictDB's default property file for 1) default port and 2) JDBC class name.

The default property file is located at *\<verdict_root\>/core/src/main/resources/verdict_default.properties*. You need to edit this file and add two properties like following:

```
### jdbc > connection > hive2
verdict.jdbc.hive2.default_port=10000
verdict.jdbc.hive2.class_name=org.apache.hive.jdbc.HiveDriver
```

The property name has the format of *verdict.jdbc.\<dbName\>.\<fieldName\>*. You need to add same properties for your driver.

### 5. Add a maven dependency for the JDBC driver

You need to add a maven dependency for the JDBC driver into **pom.xml** that can be found in the directory: *\<verdict_root\>/core*.

For example, we have the following maven dependency for Hive JDBC driver.

```xml
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>1.2.1</version>
</dependency>
```

### 6. Test and submit a pull request for your driver
Please test your driver to make sure that VerdictDB works as intended. Feel free to contact us at <verdict-user@umich.edu> or open an issue at [Verdict's public repository](https://github.com/mozafari/verdict) if you encounter any problem that cannot be resolved.

Once you are confident with the driver, please submit a pull request for your driver at [our public repository](https://github.com/mozafari/verdict). The instructions on how to create a pull request can be found [here](https://help.github.com/articles/creating-a-pull-request/). Our developers will review the request and merge your driver into Verdict. We would like to thank you for your help and contribution to VerdictDB in advance!