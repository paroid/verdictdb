# How to add a driver to Verdict for support of your own database 

This user guide explains steps required to add a driver for your database into Verdict.

## Prerequisites
1. A JDBC driver should be available for the database that you want to add a driver for.
1. The database must support functions that Verdict requires for its operations (e.g., *rand()*). This will be explained later in the guide.

## Steps
### 1. Fork the Verdict's public repository
You can do this by visiting [Verdict's public repository](https://github.com/mozafari/verdict) and clicking **Fork** button at the top-right. You need to add a driver into your forked repository and make a pull request to the public repository.

### 2. Add a driver
A driver for each DBMS that Verdict supports is included under the package: **edu.umich.verdict.dbms**. 

The package is located at: *\<verdict_root\>/core/src/main/java/edu/umich/verdict/dbms*

Basically, you need to create a Java class that extends **DbmsJDBC** class, which again inherits **Dbms** class. Since **Dbms** class contains a number of abstract functions that are not implemented by its **DbmsJDBC** subclass, you should add implementations for those abstract functions. 

Note that it may be necessary to also override some of the functions already implemented in either **Dbms** or **DbmsJDBC** classes if your database does not work with the default behavior defined in the two superclasses.

#### 2.1. Create a driver class

Create a java class file Dbms*\<db_name\>*.java under the package **edu.umich.verdict.dbms** (e.g., *DbmsMysql.java*). The class should look something like this:

```java
package edu.umich.verdict.dbms;

public class DbmsMySQL extends DbmsJDBC {
	/// member functions...
}
```

#### 2.2. Implement abstract functions

Currently, there are five abstract functions that you must provide an implementation:

1. *protected String randomPartitionColumn()*: returns a string expression that goes into SELECT statement, which will give a random partition number for each row.
1. *protected String randomNumberExpression(SampleParam param)*: returns a string expression that goes into SELECT statement, which will give a random number.
1. *public String modOfHash(String col, int mod)*: returns a string expression that goes into SELECT statement. The expression should get a hash value by applying a hash function, which is provided by the database, to the column **col**, then perform a modulo operation on the hash value with **mod** as a divisor.
1. *public String modOfHash(List\<String\> columns, int mod)*: returns a string expression that goes into SELECT statement. The expression should get a hash value by applying a hash function, which is provided by the database, to all columns listed in **columns**, then perform a modulo operation on the hash value with **mod** as a divisor.
1. *protected String modOfRand(int mod)*: returns a string expression that goes into SELECT statement, which will give a (random number modulo **mod**).

The easiest way to implement above functions is to ***look at how existing drivers implemented these functions*** (e.g., *DbmsHive.java, DbmsImpala.java*) and apply them to your driver.

Note that Verdict utilizes the functions provided by databases (e.g., *rand(), crc32(), etc.*). It is likely that Verdict will not be able to support your database if the database does not provide such functions. If this is the case, please contact us at <verdict-user@umich.edu> to discuss any possible workarounds.

#### 2.3. Override existing functions (only if necessary)

Unfortunately, your database may not support full SQL syntax and thus, the default behavior defined in Verdict might not work with your database. In such cases, you may need to override necessary functions in the *Dbms* or *DbmsJDBC* classes.

For example, the function, *public void insertEntry(TableUniqueName tableName, List\<Object\> values)*, inserts **values** into the table using the standard SQL INSERT statement: *INSERT INTO TABLE \<tableName\> WITH VALUES (...)*

The problem is that some SQL-on-hadoop engines do not support such INSERT statement (e.g., older versions of Hive). Therefore, we overrode the function for Verdict's Hive driver.

We recommend to implement abstract functions first, then override existing function if there is any problem with default functions during the test.

#### 2.4. Add the driver in Dbms.java

In *getInstance()* function under *Dbms* class, there is an if-else-if statements for instantiating a correct driver for the given database:

```java
    protected static Dbms getInstance(VerdictContext vc, String dbName, String host, String port, String schema,
            String user, String password, String jdbcClassName) throws VerdictException {

        Dbms dbms = null;

        if (dbName.equals("impala")) {
            dbms = new DbmsImpala(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("hive") || dbName.equals("hive2")) {
            dbms = new DbmsHive(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("redshift")) {
            dbms = new DbmsRedshift(vc, dbName, host, port, schema, user, password, jdbcClassName);
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

You should add an else-if statement for your driver like following (suppose you added a MySQL driver):

```java
    protected static Dbms getInstance(VerdictContext vc, String dbName, String host, String port, String schema,
            String user, String password, String jdbcClassName) throws VerdictException {

        Dbms dbms = null;

        if (dbName.equals("impala")) {
            dbms = new DbmsImpala(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("hive") || dbName.equals("hive2")) {
            dbms = new DbmsHive(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("redshift")) {
            dbms = new DbmsRedshift(vc, dbName, host, port, schema, user, password, jdbcClassName);
        } else if (dbName.equals("dummy")) {
            dbms = new DbmsDummy(vc);
        /// added MySQL driver
        } else if (dbName.equals("mysql")) {
            dbms = new DbmsMySQL(vc, dbName, host, port, schema, user, password, jdbcClassName);        
        } else {
            String msg = String.format("Unsupported DBMS: %s", dbName);
            VerdictLogger.error("Dbms", msg);
            throw new VerdictException(msg);
        }

        return dbms;
    }
```

Note that *dbName* directly goes into its JDBC connection string (e.g., *jdbc:\<dbName\>://\<host\>:\<port\>/\<schema\>*).

### 3. Test your driver
Please test your driver to make sure that Verdict works as intended. Feel free to contact us at <verdict-user@umich.edu> or open an issue at [Verdict's public repository](https://github.com/mozafari/verdict) if you encounter any problem that cannot be resolved.

### 4. Submit a pull request
Once you are confident with the driver, please submit a pull request for your driver at [our public repository](https://github.com/mozafari/verdict). Our developers will review the request and merge your driver into Verdict. We would like to thank you for your help and contribution to Verdict in advance!