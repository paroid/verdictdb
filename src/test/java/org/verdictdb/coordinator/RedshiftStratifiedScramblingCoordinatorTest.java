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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RedshiftStratifiedScramblingCoordinatorTest {

  private static Connection redshiftConn;

  private static Statement dbmsConn;

  static VerdictOption options = new VerdictOption();

  private static Statement stmt;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA =
      "stratified_scrambling_coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  @BeforeClass
  public static void setupRedshiftDatabase()
      throws SQLException, VerdictDBDbmsException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    redshiftConn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA);
    stmt = redshiftConn.createStatement();
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    JdbcConnection conn = JdbcConnection.create(redshiftConn);
    //    conn.setOutputDebugMessage(true);
    DbmsQueryResult result =
        conn.execute(String.format("select * from \"%s\".\"lineitem\"", REDSHIFT_SCHEMA));
    int rowCount = 0;
    while (result.next()) {
      rowCount++;
    }
    assertEquals(1000, rowCount);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("drop schema if exists \"%s\" CASCADE", REDSHIFT_SCHEMA));
  }

  @Test
  public void testScramblingCoordinator() throws VerdictDBException, SQLException {
    DbmsConnection conn = JdbcConnection.create(redshiftConn);
    String tablename = "lineitem";
    String columnname = "l_discount";
    String scrambleSchema = REDSHIFT_SCHEMA;
    String scratchpadSchema = REDSHIFT_SCHEMA;
    long blockSize = 100;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(conn, scrambleSchema, scratchpadSchema, blockSize);

    // perform scrambling
    String originalSchema = REDSHIFT_SCHEMA;
    String originalTable = tablename;
    String scrambledTable = tablename + "_scrambled";
    conn.execute(String.format("drop table if exists %s.%s", REDSHIFT_SCHEMA, scrambledTable));
    ScrambleMeta meta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "stratified",
        columnname, 0.1, null, Arrays.asList(columnname), 7, new HashMap<String, String>());

    // tests
    List<Pair<String, String>> originalColumns = conn.getColumns(REDSHIFT_SCHEMA, originalTable);
    List<Pair<String, String>> columns = conn.getColumns(REDSHIFT_SCHEMA, scrambledTable);
    for (int i = 0; i < originalColumns.size(); i++) {
      assertEquals(originalColumns.get(i).getLeft(), columns.get(i).getLeft());
      assertEquals(originalColumns.get(i).getRight(), columns.get(i).getRight());
    }
    assertEquals(originalColumns.size() + 2, columns.size());

    List<String> partitions = conn.getPartitionColumns(REDSHIFT_SCHEMA, scrambledTable);
    assertEquals(Arrays.asList("verdictdbblock"), partitions);

    DbmsQueryResult result1 =
        conn.execute(String.format("select count(*) from %s.%s", REDSHIFT_SCHEMA, originalTable));
    DbmsQueryResult result2 =
        conn.execute(String.format("select count(*) from %s.%s", REDSHIFT_SCHEMA, scrambledTable));
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
                REDSHIFT_SCHEMA, scrambledTable));
    int count = 0;
    while (reader.hasNext()) {
      reader.next();
      count++;
    }
    assertEquals(10, count);

    // check block 0 contains all groups
    int groupNumber = 0;
    ResultSet rs1 = stmt.executeQuery(String.format(
        "select count(distinct %s) from %s.%s", columnname, REDSHIFT_SCHEMA, originalTable));
    rs1.next();
    groupNumber = rs1.getInt(1);
    ResultSet rs2 = stmt.executeQuery(String.format(
        "select count(distinct %s) from %s.%s where verdictdbblock = 0", columnname, REDSHIFT_SCHEMA, scrambledTable));
    rs2.next();
    assertEquals(groupNumber, rs2.getInt(1));

    // check group-by query on stratified scrambles doesn't miss any groups
    reader =
        coordinator.process(
            String.format("select count(*) from %s.%s group by %s order by %s",
                REDSHIFT_SCHEMA, scrambledTable, columnname, columnname));
    DbmsQueryResult dbmsQueryResult = reader.next();
    int row = 0;
    while (dbmsQueryResult.next()) {
      row++;
    }
    assertEquals(groupNumber, row);
  }

}
