package org.verdictdb.coordinator;

import org.apache.commons.lang3.RandomStringUtils;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ImpalaStratifiedScramblingCoordinatorTest {

  private static Connection impalaConn;

  private static Statement impalaStmt;

  private static final String IMPALA_HOST;

  static VerdictOption options = new VerdictOption();

  // to avoid possible conflicts among concurrent tests
  private static final String IMPALA_DATABASE =
      "stratified_scrambling_coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String IMPALA_USER = "";

  private static final String IMPALA_PASSWORD = "";

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupImpalaDatabase()
      throws SQLException, VerdictDBDbmsException, IOException {
    String impalaConnectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    impalaConn =
        DatabaseConnectionHelpers.setupImpala(
            impalaConnectionString, IMPALA_USER, IMPALA_PASSWORD, IMPALA_DATABASE);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    impalaConn
        .createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    impalaConn.close();
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection dbmsConn = JdbcConnection.create(impalaConn);

    DbmsQueryResult rs =
        dbmsConn.execute(String.format("select * from %s.lineitem", IMPALA_DATABASE));
    assertEquals(1000, rs.getRowCount());
  }

  @Test
  public void testScramblingCoordinatorLineitem() throws VerdictDBException {
    testScramblingCoordinator("lineitem", "l_quantity");
  }

  public void testScramblingCoordinator(String tablename, String columnname) throws VerdictDBException {
    JdbcConnection conn = JdbcConnection.create(impalaConn);
    //    conn.setOutputDebugMessage(true);

    String scrambleSchema = IMPALA_DATABASE;
    String scratchpadSchema = IMPALA_DATABASE;
    long blockSize = 100;
    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = IMPALA_DATABASE;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", IMPALA_DATABASE, scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "stratified",
        columnname, 0.1, null, Arrays.asList(columnname), 1, new HashMap<String, String>());

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(IMPALA_DATABASE, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(IMPALA_DATABASE, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size() + 2, columns.size());

    List<String> partitions = conn.getPartitionColumns(IMPALA_DATABASE, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", IMPALA_DATABASE, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", IMPALA_DATABASE, scrambledTable));
    result1.next();
    result2.next();
    assertEquals(result1.getInt(0), result2.getInt(0));

    //assertEquals((int) Math.ceil(result2.getInt(0) / (float) blockSize) - 1, result.getInt(1));

    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(conn, options);
    ScrambleMetaSet scrambleMetas = new ScrambleMetaSet();
    scrambleMetas.addScrambleMeta(meta);
    coordinator.setScrambleMetaSet(scrambleMetas);
    ExecutionResultReader reader =
        coordinator.process(
            String.format("select count(*) from %s.%s",
                IMPALA_DATABASE, scrambledTable));
    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }
    assertEquals(10, count);
  }
}
