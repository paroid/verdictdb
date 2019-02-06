package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.jdbc41.VerdictConnection;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.assertEquals;

public class MySQLStratifiedScramblingCreateTest {

  private static VerdictConnection verdictConn;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE =
      "stratified_scrambling_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  private static Statement stmt, vstmt;

  @BeforeClass
  public static void setupMySQLdatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    stmt = conn.createStatement();
    mysqlConnectionString =
        String.format("jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    verdictConn =
        (VerdictConnection)
            DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    vstmt = verdictConn.createStatement();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
  }

  @Test
  public void createStratifiedScramblingTest() throws SQLException, VerdictDBException, IOException {
    vstmt.execute(String.format("use %s", MYSQL_DATABASE));
    stmt.execute(String.format("use %s", MYSQL_DATABASE));
    vstmt.execute("create scramble lineitem_stratified1 from lineitem METHOD stratified ON l_quantity RATIO 0.1 LEASTSAMPLINGSIZE 1");
    ResultSet rs = stmt.executeQuery("select count(*) from lineitem_stratified1");
    rs.next();
    assertEquals(1000, rs.getInt(1));
  }

  @Test
  public void createStratifiedScramblingMultipleColumnsTest() throws SQLException, VerdictDBException, IOException {
    vstmt.execute(String.format("use %s", MYSQL_DATABASE));
    stmt.execute(String.format("use %s", MYSQL_DATABASE));
    vstmt.execute("create scramble lineitem_stratified2 from lineitem METHOD stratified ON l_quantity, l_partkey RATIO 0.1 LEASTSAMPLINGSIZE 1");
    ResultSet rs = stmt.executeQuery("select count(*) from lineitem_stratified2");
    rs.next();
    assertEquals(1000, rs.getInt(1));
  }
}
