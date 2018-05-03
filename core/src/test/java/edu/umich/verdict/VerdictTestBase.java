package edu.umich.verdict;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Dong Young Yoon on 5/2/18.
 */
public abstract class VerdictTestBase {

    protected Connection conn;

    protected static boolean isSetup = false;

    @BeforeClass
    public void setUp() throws Exception {
        conn = DriverManager.getConnection("jdbc:h2:mem:verdict_test;DB_CLOSE_DELAY=-1");

        if (!isSetup) {
            // load some data into embedded in-memory DB

        }

    }

    @AfterClass
    public void tearDown() throws Exception {
        conn.close();
    }
}
