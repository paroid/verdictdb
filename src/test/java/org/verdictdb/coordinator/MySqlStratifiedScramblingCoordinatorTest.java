package org.verdictdb.coordinator;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MySqlStratifiedScramblingCoordinatorTest {

  private static Connection mysqlConn;

  private static Statement mysqlStmt;

  static VerdictOption options = new VerdictOption();

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "stratified_scrambling_coordinator_test";

  private static final String MYSQL_UESR;

  private static final String MYSQL_PASSWORD = "";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
      MYSQL_UESR = "root";
    } else {
      MYSQL_HOST = "localhost";
      MYSQL_UESR = "root";
    }
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    mysqlConn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    mysqlStmt = mysqlConn.createStatement();

  }

  @AfterClass
  public static void tearDown() throws SQLException {
    mysqlStmt.execute(String.format("drop schema if exists `%s`", MYSQL_DATABASE));
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection conn = JdbcConnection.create(mysqlConn);
    DbmsQueryResult result = conn.execute(String.format("select * from `%s`.lineitem", MYSQL_DATABASE));
    int rowCount = 0;
    while (result.next()) {
      rowCount++;
    }
    assertEquals(1000, rowCount);
  }

  @Test
  public void testScramblingCoordinatorLineitem() throws VerdictDBException {
    testScramblingCoordinator("lineitem", "l_quantity");
  }


  public void testScramblingCoordinator(String tablename, String columnname) throws VerdictDBException {
    DbmsConnection conn = JdbcConnection.create(mysqlConn);

    String scrambleSchema = MYSQL_DATABASE;
    String scratchpadSchema = MYSQL_DATABASE;
    long blockSize = 100;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = MYSQL_DATABASE;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", MYSQL_DATABASE, scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "stratified",
        columnname, 0.1, null, Arrays.asList(columnname), 1, new HashMap<String, String>());

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(MYSQL_DATABASE, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(MYSQL_DATABASE, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size()+2, columns.size());

    List<String> partitions = conn.getPartitionColumns(MYSQL_DATABASE, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", MYSQL_DATABASE, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", MYSQL_DATABASE, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(conn, options);
    ScrambleMetaSet scrambleMetas = new ScrambleMetaSet();
    scrambleMetas.addScrambleMeta(meta);
    coordinator.setScrambleMetaSet(scrambleMetas);
    ExecutionResultReader reader =
        coordinator.process(
            String.format("select count(*) from %s.%s",
                MYSQL_DATABASE, scrambledTable));
    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }
    // has 11 blocks, but block0 should not return result.
    assertEquals(10, count);
  }
}
