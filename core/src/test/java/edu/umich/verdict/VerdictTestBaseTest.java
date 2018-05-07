package edu.umich.verdict;

import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.exceptions.VerdictException;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by Dong Young Yoon on 5/7/18.
 */
public class VerdictTestBaseTest extends VerdictTestBase {

  private final static long CUSTOMER_ROW_COUNT = 7500;
  private final static long LINEITEM_ROW_COUNT = 299814;
  private final static long NATION_ROW_COUNT = 25;
  private final static long ORDERS_ROW_COUNT = 75000;
  private final static long PART_ROW_COUNT = 10000;
  private final static long PARTSUPP_ROW_COUNT = 40000;
  private final static long REGION_ROW_COUNT = 5;
  private final static long SUPPLIER_ROW_COUNT = 500;

  @Test
  public void setUpSchemaProperly() throws VerdictException, SQLException {

    DbmsJDBC dbms = vc.getDbms().getDbmsJDBC();
    Statement stmt = dbms.createStatement();

    // test customer
    ResultSet rs = stmt.executeQuery("select count(*) from customer");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(CUSTOMER_ROW_COUNT, rowCount);
    } else {
      fail("customer table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from lineitem");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(LINEITEM_ROW_COUNT, rowCount);
    } else {
      fail("lineitem table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from nation");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(NATION_ROW_COUNT, rowCount);
    } else {
      fail("nation table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from orders");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(ORDERS_ROW_COUNT, rowCount);
    } else {
      fail("orders table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from part");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(PART_ROW_COUNT, rowCount);
    } else {
      fail("part table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from partsupp");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(PARTSUPP_ROW_COUNT, rowCount);
    } else {
      fail("partsupp table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from region");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(REGION_ROW_COUNT, rowCount);
    } else {
      fail("region table not loaded properly");
    }

    rs = stmt.executeQuery("select count(*) from supplier");
    if (rs.next()) {
      long rowCount = rs.getLong(1);
      assertEquals(SUPPLIER_ROW_COUNT, rowCount);
    } else {
      fail("supplier table not loaded properly");
    }
    stmt.close();
  }
}
