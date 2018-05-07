package edu.umich.verdict;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import edu.umich.verdict.exceptions.VerdictException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;

/**
 * Created by Dong Young Yoon on 5/2/18.
 */
public abstract class VerdictTestBase {

    protected static VerdictContext vc;

    protected static boolean isSetup = false;

    protected final static String RESOURCE_PATH = "./src/test/resources/tpch";

    protected final static String SCHEMA_FILE = "tpch-schema.sql";

    @BeforeClass
    public static void setUp() throws Exception {
        if (!isSetup) {
            VerdictConf conf = new VerdictConf();
            conf.setDbms("h2mem");
            conf.setDbmsSchema("test");
            conf.setLoglevel("info");
            vc = VerdictJDBCContext.from(conf);

            // setup tpc-h schema
            File schemaFile = new File(RESOURCE_PATH + File.separator + SCHEMA_FILE);
            String schemas = Files.toString(schemaFile, Charsets.UTF_8);
            for (String schema : schemas.split(";")) {
                schema += ";"; // add semicolon at the end
                schema = schema.trim();

                vc.getDbms().execute(schema);
            }

            // load some tpc-h data into embedded in-memory DB
            loadData();
            isSetup = true;
        }
    }

    private static void loadData() throws VerdictException {
        String loadNation = String.format("INSERT INTO nation SELECT * FROM " +
                        "CSVREAD('%s/nation.tbl'," +
                        "'n_nationkey|n_name|n_regionkey|n_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadRegion = String.format("INSERT INTO region SELECT * FROM " +
                        "CSVREAD('%s/region.tbl'," +
                        "'r_regionkey|r_name|r_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadPart = String.format("INSERT INTO part SELECT * FROM " +
                        "CSVREAD('%s/part.tbl'," +
                        "'p_partkey|p_name|p_mgfr|p_brand|p_type|p_size|p_container|p_retailprice|p_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadSupplier = String.format("INSERT INTO supplier SELECT * FROM " +
                        "CSVREAD('%s/supplier.tbl'," +
                        "'s_suppkey|s_name|s_address|s_nationkey|s_phone|s_acctbal|s_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadPartsupp = String.format("INSERT INTO partsupp SELECT * FROM " +
                        "CSVREAD('%s/partsupp.tbl'," +
                        "'ps_partkey|ps_suppkey|ps_availqty|ps_supplycost|ps_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadCustomer = String.format("INSERT INTO customer SELECT * FROM " +
                        "CSVREAD('%s/customer.tbl'," +
                        "'c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadOrders = String.format("INSERT INTO orders SELECT * FROM " +
                        "CSVREAD('%s/orders.tbl'," +
                        "'o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderDATE|o_orderpriority|o_clerk|o_shippriority|o_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        String loadLineitem = String.format("INSERT INTO lineitem SELECT * FROM " +
                        "CSVREAD('%s/lineitem.tbl'," +
                        "'l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipDATE|l_commitDATE|l_receiptDATE|l_shipinstruct|l_shipmode|l_comment'," +
                        "'fieldSeparator=|')",
                RESOURCE_PATH);
        vc.getDbms().execute(loadNation);
        vc.getDbms().execute(loadRegion);
        vc.getDbms().execute(loadPart);
        vc.getDbms().execute(loadSupplier);
        vc.getDbms().execute(loadPartsupp);
        vc.getDbms().execute(loadCustomer);
        vc.getDbms().execute(loadOrders);
        vc.getDbms().execute(loadLineitem);
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }
}
