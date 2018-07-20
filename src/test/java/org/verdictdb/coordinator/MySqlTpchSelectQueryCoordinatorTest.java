package org.verdictdb.coordinator;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

/**
 *  Test cases are from
 *  https://github.com/umich-dbgroup/verdictdb-core/wiki/TPCH-Query-Reference--(Experiment-Version)
 *
 *  Some test cases are slightly changed because size of test data are small.
 */
public class MySqlTpchSelectQueryCoordinatorTest {

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  final static int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

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

  private static final String MYSQL_DATABASE = "coordinator_test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    stmt = conn.createStatement();
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));
    
    ScramblingCoordinator scrambler = 
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 = 
        scrambler.scramble(MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 = 
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);

    stmt.execute("create schema if not exists `verdictdb_temp`");
  }

  Pair<ExecutionResultReader, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    String filename = "query"+queryNum+".sql";
    File file = new File("src/test/resources/tpch_test_query/"+filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(conn, new MysqlSyntax()));
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);
    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    ResultSet rs = stmt.executeQuery(stdQuery);
    return new ImmutablePair<>(reader, rs);
  }

  @Test
  public void query1Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(1);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getLong(3), dbmsQueryResult.getLong(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-5);
          assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-5);
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-5);
          assertEquals(rs.getDouble(7), dbmsQueryResult.getDouble(6), 1e-5);
          assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-5);
          assertEquals(rs.getDouble(9), dbmsQueryResult.getDouble(8), 1e-5);
          assertEquals(rs.getDouble(10), dbmsQueryResult.getDouble(9), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query3Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(3);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query4Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(4);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query5Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(5);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query6Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(6);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query7Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(7);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query8Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(8);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query9Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(9);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query10Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(10);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }


  @Test
  public void query12Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(12);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query13Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(13);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 3) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(3, cnt);
  }

  @Test
  public void query14Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(14);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query15Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(15);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query17Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(17);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query18Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(18);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
          assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }


  @Test
  public void query19Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(19);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query20Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(20);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }


  @Test
  public void query21Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(21);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
  }
}
