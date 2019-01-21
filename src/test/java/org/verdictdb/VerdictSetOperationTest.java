package org.verdictdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.coordinator.SelectQueryCoordinator;
import org.verdictdb.coordinator.VerdictResultStreamFromExecutionResultReader;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VerdictSetOperationTest {
  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static VerdictOption options = new VerdictOption();

  static Connection conn;

  private static Statement stmt;

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
      "mysql_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    conn.setCatalog(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute(String.format("use `%s`", MYSQL_DATABASE));
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));
    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
    ScrambleMeta meta3 =
        scrambler.scramble(
            MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_hash_scrambled", "hash", "o_orderkey");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    meta.addScrambleMeta(meta3);
    stmt.execute(String.format("drop schema if exists `%s`", options.getVerdictTempSchemaName()));
    stmt.execute(
        String.format("create schema if not exists `%s`", options.getVerdictTempSchemaName()));
  }


  @Test
  public void testUnion() throws VerdictDBException {
    String sql = String.format(
        "select count(o_orderkey) from " +
            "((select o_orderkey from %s.orders_scrambled where MOD(o_orderkey, 2) = 0) UNION ALL (select o_orderkey from %s.orders_scrambled where MOD(o_orderkey, 2) = 1)) as t",
        MYSQL_DATABASE, MYSQL_DATABASE);
    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      for (int i=0;i<3;i++) {
        VerdictSingleResult rs = stream.next();
        rs.next();
        if (i==2) {
          assertEquals(258, rs.getInt(0));
        }
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Test
  public void testUnionALL() throws VerdictDBException {
    String sql = String.format(
        "select count(o_orderkey) from " +
            "((select o_orderkey from %s.orders_scrambled) UNION ALL (select o_orderkey from %s.orders_scrambled)) as t",
        MYSQL_DATABASE, MYSQL_DATABASE);
    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      for (int i=0;i<3;i++) {
        VerdictSingleResult rs = stream.next();
        rs.next();
        if (i==2) {
          assertEquals(516, rs.getInt(0));
        }
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @Test
  public void testUnionThree() throws VerdictDBException {
    String sql = String.format(
        "select count(o_orderkey) from " +
            "((select o_orderkey from %s.orders_scrambled where MOD(o_orderkey, 5) = 0) " +
            "UNION (select o_orderkey from %s.orders_scrambled where MOD(o_orderkey, 5) = 1) " +
            "UNION (select o_orderkey from %s.orders_scrambled where MOD(o_orderkey, 5) = 2)) as t",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);
    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      for (int i=0;i<3;i++) {
        VerdictSingleResult rs = stream.next();
        rs.next();
        if (i==2) {
          assertEquals(157, rs.getInt(0));
        }
      }
    } catch (RuntimeException e) {
      throw e;
    }
  }


  /**
   * Test cases when two scramble tables have different probability distribution,
   * VerdictDB will not handle this situation and will execute query without scrambling.
   * @throws VerdictDBException
   */
  @Test
  public void testDifferentProbDistribution() throws VerdictDBException {
    String sql = String.format(
        "select count(orderkey) from " +
            "((select o_orderkey as orderkey from %s.orders_scrambled) " +
            "UNION (select l_orderkey as orderkey from %s.lineitem_scrambled)) as t",
        MYSQL_DATABASE, MYSQL_DATABASE, MYSQL_DATABASE);
    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    try {
      VerdictSingleResult rs = stream.next();
      assertFalse(stream.hasNext());
      rs.next();
      assertEquals(258, rs.getInt(0));
    } catch (RuntimeException e) {
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
  }
}
