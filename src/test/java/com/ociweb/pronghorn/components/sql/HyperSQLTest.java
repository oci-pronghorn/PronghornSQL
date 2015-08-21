package com.ociweb.pronghorn.components.sql;

import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.deleteDirectory;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.deleteFile;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.executeSQL;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.metaFROM;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.runTest;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.getDate;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.unicodeTwoHeartsGlyph;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.components.sql.DBUtil.MetaDumper;
import com.ociweb.pronghorn.components.sql.HyperSQLComponent.HyperSQLStage;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HyperSQLTest {
    private static String hsqldbDB = "hsqldbTest";

    // http://www.h2database.com/html/quickstart.html
    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        // Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:hsqldb:file:./" + hsqldbDB);
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        deleteFile(hsqldbDB + ".lck");
        deleteFile(hsqldbDB + ".log");
        deleteFile(hsqldbDB + ".properties");
        deleteFile(hsqldbDB + ".script");
        deleteDirectory(hsqldbDB + ".tmp");

        // create the database and table
        Connection conn = getConnection();
        conn.setAutoCommit(false);

        // http://hsqldb.org/doc/2.0/guide/sqlgeneral-chapt.html#sgc_types_ops
        setupINTEGER(conn);
        setupBIT(conn);
        setupBIT2(conn);
        setupBITVARYING2(conn);
        setupBOOLEAN(conn);
        setupTINYINT(conn);
        setupSMALLINT(conn);
        setupBIGINT(conn);
        setupDECIMAL(conn);
        setupDOUBLE(conn);
        setupREAL(conn);
        setupVARBINARY(conn);
        setupOTHER(conn);
        setupBLOB(conn);
        setupARRAY(conn);
        setupDATE(conn);
        setupTIME(conn);
        setupTIMESTAMP(conn);
        setupTIMESTAMPWITHTIMEZONE(conn);
        setupCHAR30(conn);
        setupVARCHAR30(conn);
        setupCLOB(conn);

        conn.commit();
        conn.close();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Connection conn = getConnection();
        Statement st = conn.createStatement();
        // db writes out to files and performs clean shuts down
        // otherwise there will be an unclean shutdown
        // when program ends
        st.execute("SHUTDOWN");
        st.close();
        conn.close();
    }

    private List<Object> runMetaTest(String sql, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + sql + "' " + emitFieldNames + " " + emitRowMarkers);
        Connection conn = getConnection();
        try {
            RingBuffer output = new RingBuffer(new RingBufferConfig((byte) 10, (byte) 24, null, metaFROM));
            GraphManager gm = new GraphManager();
            HyperSQLStage stage = new HyperSQLStage(conn, sql, emitFieldNames, emitRowMarkers, output);
            MetaDumper dumper = new MetaDumper(gm, output);
            return runTest(stage, dumper);
        } finally {
            conn.close();
        }
    }

    private List<Object> runMetaTest(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + stmt.toString() + "' " + emitFieldNames + " " + emitRowMarkers);
        RingBuffer output = new RingBuffer(new RingBufferConfig((byte) 10, (byte) 24, null, metaFROM));
        GraphManager gm = new GraphManager();
        HyperSQLStage stage = new HyperSQLStage(stmt, emitFieldNames, emitRowMarkers, output);
        MetaDumper dumper = new MetaDumper(gm, output);
        return runTest(stage, dumper);
    }

    private static void setupINTEGER(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE INTEGERData (Field INTEGER NOT NULL, FieldNullable INTEGER);");
        executeSQL(conn, "INSERT INTO INTEGERData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO INTEGERData (Field, FieldNullable) VALUES (1, 2);");
    }

    @Test
    public void testINTEGER() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTEGERData", false, false);
        assertEquals(4, result.size());
        assertEquals(0, result.get(0));
        assertNull(result.get(1));
        assertEquals(1, result.get(2));
        assertEquals(2, result.get(3));
    }

    @Test
    public void testINTEGER_rowmarkers() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTEGERData", false, true);
        assertEquals(8, result.size());
        assertEquals("**BEGIN GROUP**", result.get(0));
        assertEquals(0, result.get(1));
        assertNull(result.get(2));
        assertEquals("**END GROUP**", result.get(3));
        assertEquals("**BEGIN GROUP**", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals(2, result.get(6));
        assertEquals("**END GROUP**", result.get(7));
    }

    @Test
    public void testINTEGER_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTEGERData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(2, result.get(7));
    }

    @Test
    public void testINTEGER_names_rowmarkers() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTEGERData", true, true);
        assertEquals(12, result.size());
        assertEquals("**BEGIN GROUP**", result.get(0));
        assertEquals("FIELD", result.get(1));
        assertEquals(0, result.get(2));
        assertEquals("FIELDNULLABLE", result.get(3));
        assertNull(result.get(4));
        assertEquals("**END GROUP**", result.get(5));
        assertEquals("**BEGIN GROUP**", result.get(6));
        assertEquals("FIELD", result.get(7));
        assertEquals(1, result.get(8));
        assertEquals("FIELDNULLABLE", result.get(9));
        assertEquals(2, result.get(10));
        assertEquals("**END GROUP**", result.get(11));
    }

    @Test
    public void testINTEGER_prepared() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement("SELECT Field, FieldNullable FROM INTEGERData");
            List<Object> result = runMetaTest(stmt, false, false);
            assertEquals(4, result.size());
            assertEquals(0, result.get(0));
            assertNull(result.get(1));
            assertEquals(1, result.get(2));
            assertEquals(2, result.get(3));
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static void setupBIT(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE BITData (Field BIT NOT NULL, FieldNullable BIT);");
        executeSQL(conn, "INSERT INTO BITData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO BITData (Field, FieldNullable) VALUES (1, 0);");
    }

    @Test
    public void testBIT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BITData", false, false);
        assertEquals(4, result.size());
        assertEquals(false, result.get(0));
        assertNull(result.get(1));
        assertEquals(true, result.get(2));
        assertEquals(false, result.get(3));
    }

    @Test
    public void testBIT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BITData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(false, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(true, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(false, result.get(7));
    }

    private static void setupBIT2(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE BIT2Data (Field BIT(2) NOT NULL, FieldNullable BIT(2));");
        executeSQL(conn, "INSERT INTO BIT2Data (Field, FieldNullable) VALUES (B'00', NULL);");
        executeSQL(conn, "INSERT INTO BIT2Data (Field, FieldNullable) VALUES (B'10', B'11');");
    }

    @Test
    public void testBIT2() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BIT2Data", false, false);
        assertEquals(4, result.size());
        assertArrayEquals(new byte[] { 0 }, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(new byte[] { -128 }, (byte[]) result.get(2));
        assertArrayEquals(new byte[] { -64 }, (byte[]) result.get(3));
    }

    @Test
    public void testBIT2_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BIT2Data", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertArrayEquals(new byte[] { 0 }, (byte[]) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertArrayEquals(new byte[] { -128 }, (byte[]) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertArrayEquals(new byte[] { -64 }, (byte[]) result.get(7));
    }

    private static void setupBITVARYING2(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE BITVARYING2Data (Field BIT VARYING(2) NOT NULL, FieldNullable BIT VARYING(2));");
        executeSQL(conn, "INSERT INTO BITVARYING2Data (Field, FieldNullable) VALUES (B'00', NULL);");
        executeSQL(conn, "INSERT INTO BITVARYING2Data (Field, FieldNullable) VALUES (B'10', B'11');");
    }

    @Test
    public void testBITVARYING2() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BITVARYING2Data", false, false);
        assertEquals(4, result.size());
        assertArrayEquals(new byte[] { 0 }, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(new byte[] { -128 }, (byte[]) result.get(2));
        assertArrayEquals(new byte[] { -64 }, (byte[]) result.get(3));
    }

    @Test
    public void testBITVARYING2_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BITVARYING2Data", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertArrayEquals(new byte[] { 0 }, (byte[]) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertArrayEquals(new byte[] { -128 }, (byte[]) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertArrayEquals(new byte[] { -64 }, (byte[]) result.get(7));
    }

    private static void setupBOOLEAN(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE BOOLEANData (Field BOOLEAN NOT NULL, FieldNullable BOOLEAN);");
        executeSQL(conn, "INSERT INTO BOOLEANData (Field, FieldNullable) VALUES (FALSE, NULL);");
        executeSQL(conn, "INSERT INTO BOOLEANData (Field, FieldNullable) VALUES (TRUE, FALSE);");
    }

    @Test
    public void testBOOLEAN() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BOOLEANData", false, false);
        assertEquals(4, result.size());
        assertEquals(false, result.get(0));
        assertNull(result.get(1));
        assertEquals(true, result.get(2));
        assertEquals(false, result.get(3));
    }

    @Test
    public void testBOOLEAN_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BOOLEANData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(false, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(true, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(false, result.get(7));
    }

    private static void setupTINYINT(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE TINYINTData (Field TINYINT NOT NULL, FieldNullable TINYINT);");
        executeSQL(conn, "INSERT INTO TINYINTData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO TINYINTData (Field, FieldNullable) VALUES (1, 2);");
    }

    @Test
    public void testTINYINT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TINYINTData", false, false);
        assertEquals(4, result.size());
        assertEquals(0, result.get(0));
        assertNull(result.get(1));
        assertEquals(1, result.get(2));
        assertEquals(2, result.get(3));
    }

    @Test
    public void testTINYINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TINYINTData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(2, result.get(7));
    }

    private static void setupSMALLINT(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE SMALLINTData (Field SMALLINT NOT NULL, FieldNullable SMALLINT);");
        executeSQL(conn, "INSERT INTO SMALLINTData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO SMALLINTData (Field, FieldNullable) VALUES (1, 2);");
    }

    @Test
    public void testSMALLINT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLINTData", false, false);
        assertEquals(4, result.size());
        assertEquals(0, result.get(0));
        assertNull(result.get(1));
        assertEquals(1, result.get(2));
        assertEquals(2, result.get(3));
    }

    @Test
    public void testSMALLINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLINTData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(2, result.get(7));
    }

    private static void setupBIGINT(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE BIGINTData (Field BIGINT NOT NULL, FieldNullable BIGINT);");
        executeSQL(conn, "INSERT INTO BIGINTData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO BIGINTData (Field, FieldNullable) VALUES (1, 2);");
    }

    @Test
    public void testBIGINT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BIGINTData", false, false);
        assertEquals(4, result.size());
        assertEquals(0l, result.get(0));
        assertNull(result.get(1));
        assertEquals(1l, result.get(2));
        assertEquals(2l, result.get(3));
    }

    @Test
    public void testBIGINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BIGINTData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0l, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(1l, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(2l, result.get(7));
    }

    private static void setupDECIMAL(Connection conn) throws SQLException {
        // fixed precision and scale, java.math.BigDecimal
        executeSQL(conn, "CREATE TABLE DECIMALData (Field DECIMAL(20,2) NOT NULL, FieldNullable DECIMAL(10,4));");
        executeSQL(conn, "INSERT INTO DECIMALData (Field, FieldNullable) VALUES (0.1, NULL);");
        executeSQL(conn, "INSERT INTO DECIMALData (Field, FieldNullable) VALUES (0.2, 0.4);");
    }

    @Test
    public void testDECIMAL() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DECIMALData", false, false);
        assertEquals(4, result.size());
        assertEquals(BigDecimal.valueOf(10, 2), result.get(0));
        assertNull(result.get(1));
        assertEquals(BigDecimal.valueOf(20, 2), result.get(2));
        assertEquals(BigDecimal.valueOf(4000, 4), result.get(3));
    }

    @Test
    public void testDECIMAL_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DECIMALData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(BigDecimal.valueOf(10, 2), result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(BigDecimal.valueOf(20, 2), result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(BigDecimal.valueOf(4000, 4), result.get(7));
    }

    private static void setupDOUBLE(Connection conn) throws SQLException {
        // floating point number, java.lang.Double
        executeSQL(conn, "CREATE TABLE DOUBLEData (Field DOUBLE NOT NULL, FieldNullable DOUBLE);");
        executeSQL(conn, "INSERT INTO DOUBLEData (Field, FieldNullable) VALUES (0.1, NULL);");
        executeSQL(conn, "INSERT INTO DOUBLEData (Field, FieldNullable) VALUES (0.2, 0.4);");
    }

    @Test
    public void testDOUBLE() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DOUBLEData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1, result.get(0));
        assertNull(result.get(1));
        assertEquals(0.2, result.get(2));
        assertEquals(0.4, result.get(3));
    }

    @Test
    public void testDOUBLE_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DOUBLEData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0.1, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(0.2, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(0.4, result.get(7));
    }

    private static void setupREAL(Connection conn) throws SQLException {
        // single precision floating point number, java.lang.Float
        executeSQL(conn, "CREATE TABLE REALData (Field REAL NOT NULL, FieldNullable REAL);");
        executeSQL(conn, "INSERT INTO REALData (Field, FieldNullable) VALUES (0.1, NULL);");
        executeSQL(conn, "INSERT INTO REALData (Field, FieldNullable) VALUES (0.2, 0.4);");

    }

    @Test
    public void testREAL() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM REALData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1, (double) result.get(0), 1E-5);
        assertNull(result.get(1));
        assertEquals(0.2, (double) result.get(2), 1E-5);
        assertEquals(0.4, (double) result.get(3), 1E-5);
    }

    @Test
    public void testREAL_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM REALData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0.1, (double) result.get(1), 1E-5);
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(0.2, (double) result.get(5), 1E-5);
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(0.4, (double) result.get(7), 1E-5);
    }

    private static void setupVARBINARY(Connection conn) throws SQLException {
        // will get "java.sql.SQLDataException: data exception: string data, right truncation;"
        // if the amount of data being written is larger than the field size
        // BINARY(L) has a fixed size - length read back is L
        // VARBINARY(L) writes only what is needed
        executeSQL(conn, "CREATE TABLE VARBINARYData (Field VARBINARY(1024) NOT NULL, FieldNullable VARBINARY(1024));");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO VARBINARYData (Field, FieldNullable) values (?, ?);");
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        ps.setBytes(1, ba1);
        ps.setNull(2, Types.BINARY);
        ps.execute();
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        ps.setBytes(1, ba2);
        ps.setBytes(2, ba3);
        ps.execute();
        ps.close();
    }

    @Test
    public void testBINARY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARBINARYData", false, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        assertEquals(4, result.size());
        assertArrayEquals(ba1, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(ba2, (byte[]) result.get(2));
        assertArrayEquals(ba3, (byte[]) result.get(3));
    }

    @Test
    public void testBINARY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARBINARYData", true, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertArrayEquals(ba1, (byte[]) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertArrayEquals(ba2, (byte[]) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertArrayEquals(ba3, (byte[]) result.get(7));
    }

    private static void setupOTHER(Connection conn) throws SQLException {
        // serialized Java objects (as a byte array), java.lang.Object or subclass
        executeSQL(conn, "CREATE TABLE OTHERData (Field OTHER NOT NULL, FieldNullable OTHER);");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO OTHERData (Field, FieldNullable) values (?, ?);");
        Pair<Long, String> p1 = Pair.of(3l, "Fred");
        ps.setObject(1, p1);
        ps.setNull(2, Types.JAVA_OBJECT);
        ps.execute();
        Pair<String, Integer> p2 = Pair.of("Bob", 4);
        Pair<Short, Byte> p3 = Pair.of((short) 2, (byte) 1);
        ps.setObject(1, p2);
        ps.setObject(2, p3);
        ps.execute();
        ps.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOTHER() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM OTHERData", false, false);
        Pair<Long, String> p1 = Pair.of(3l, "Fred");
        Pair<String, Integer> p2 = Pair.of("Bob", 4);
        Pair<Short, Byte> p3 = Pair.of((short) 2, (byte) 1);
        assertEquals(4, result.size());
        assertEquals(p1, (Pair<Long, String>) result.get(0));
        assertNull(result.get(1));
        assertEquals(p2, (Pair<String, Integer>) result.get(2));
        assertEquals(p3, (Pair<Short, Byte>) result.get(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOTHER_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM OTHERData", true, false);
        Pair<Long, String> p1 = Pair.of(3l, "Fred");
        Pair<String, Integer> p2 = Pair.of("Bob", 4);
        Pair<Short, Byte> p3 = Pair.of((short) 2, (byte) 1);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(p1, (Pair<Long, String>) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(p2, (Pair<String, Integer>) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(p3, (Pair<Short, Byte>) result.get(7));
    }

    private static void setupBLOB(Connection conn) throws SQLException {
        // like BINARY, but for large values such as files or images,
        // java.sql.Blob or java.io.InputStream
        executeSQL(conn, "CREATE TABLE BLOBData (Field BLOB NOT NULL, FieldNullable BLOB);");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO BLOBData (Field, FieldNullable) values (?, ?);");
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        ps.setBytes(1, ba1); // use setBinaryStream() instead?
        ps.setNull(2, Types.BLOB);
        ps.execute();
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        ps.setBytes(1, ba2);
        ps.setBytes(2, ba3);
        ps.execute();
        ps.close();
    }

    @Test
    public void testBLOB() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BLOBData", false, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        assertEquals(4, result.size());
        assertArrayEquals(ba1, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(ba2, (byte[]) result.get(2));
        assertArrayEquals(ba3, (byte[]) result.get(3));
    }

    @Test
    public void testBLOB_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BLOBData", true, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertArrayEquals(ba1, (byte[]) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertArrayEquals(ba2, (byte[]) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertArrayEquals(ba3, (byte[]) result.get(7));
    }

    private static void setupARRAY(Connection conn) throws SQLException {
        // array of values, java.lang.Object[]
        executeSQL(conn, "CREATE TABLE ARRAYData (Field INTEGER ARRAY NOT NULL, FieldNullable INTEGER ARRAY[10]);");
        executeSQL(conn, "INSERT INTO ARRAYData (Field, FieldNullable) VALUES (ARRAY[1,2], NULL);");
        executeSQL(conn, "INSERT INTO ARRAYData (Field, FieldNullable) VALUES (ARRAY[3,4], ARRAY[5,6]);");
        // use PreparedStatement.setObject(.., new Object[] {..}) instead?
    }

    @Test
    public void testARRAY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM ARRAYData", false, false);
        assertEquals(4, result.size());
        assertArrayEquals(new Object[] { 1, 2 }, (Object[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(new Object[] { 3, 4 }, (Object[]) result.get(2));
        assertArrayEquals(new Object[] { 5, 6 }, (Object[]) result.get(3));
    }

    @Test
    public void testARRAY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM ARRAYData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertArrayEquals(new Object[] { 1, 2 }, (Object[]) result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertArrayEquals(new Object[] { 3, 4 }, (Object[]) result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertArrayEquals(new Object[] { 5, 6 }, (Object[]) result.get(7));
    }

    private static void setupTIME(Connection conn) throws SQLException {
        // hh:mm:ss, java.sql.Time - really java.util.Date
        // long getTime() - ms since 1/1/1970
        executeSQL(conn, "CREATE TABLE TIMEData (Field TIME NOT NULL, FieldNullable TIME);");
        executeSQL(conn, "INSERT INTO TIMEData (Field, FieldNullable) VALUES ('00:00:00', NULL);");
        executeSQL(conn, "INSERT INTO TIMEData (Field, FieldNullable) VALUES ('00:00:01', '00:00:02');");
    }

    @Test
    public void testTIME() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMEData", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 1, 0), result.get(2));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 2, 0), result.get(3));
    }

    @Test
    public void testTIME_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMEData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 1, 0), result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 2, 0), result.get(7));
    }

    private static void setupDATE(Connection conn) throws SQLException {
        // yyyy-MM-dd, java.sql.Date - really java.util.Date
        // long getTime() - ms since 1/1/1970
        executeSQL(conn, "CREATE TABLE DATEData (Field DATE NOT NULL, FieldNullable DATE);");
        executeSQL(conn, "INSERT INTO DATEData (Field, FieldNullable) VALUES ('2010-01-01', NULL);");
        executeSQL(conn, "INSERT INTO DATEData (Field, FieldNullable) VALUES ('2010-01-02', '2010-01-03');");
    }

    @Test
    public void testDATE() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATEData", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(3));
    }

    @Test
    public void testDATE_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATEData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(7));
    }

    private static void setupTIMESTAMP(Connection conn) throws SQLException {
        // yyyy-MM-dd hh:mm:ss[.nnnnnnnnn], java.sql.Timestamp - really java.util.Date, plus a holder for fractional
        // seconds
        // long getTime() - ms since 1/1/1970, getNanos() - nanos value
        executeSQL(conn, "CREATE TABLE TIMESTAMPData (Field TIMESTAMP NOT NULL, FieldNullable TIMESTAMP);");
        executeSQL(conn, "INSERT INTO TIMESTAMPData (Field, FieldNullable) VALUES ('2010-01-01 00:00:00.1234', NULL);");
        executeSQL(conn, "INSERT INTO TIMESTAMPData (Field, FieldNullable) VALUES ('2010-01-02 00:00:00.2345', '2010-01-03 00:00:00.3456');");
    }

    @Test
    public void testTIMESTAMP() throws Exception {
        // this should really include nanos
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMESTAMPData", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(3));
    }

    @Test
    public void testTIMESTAMP_names() throws Exception {
        // this should really include nanos
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMESTAMPData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(7));
    }

    private static void setupTIMESTAMPWITHTIMEZONE(Connection conn) throws SQLException {
        // yyyy-MM-dd hh:mm:ss[.nnnnnnnnn], java.sql.Timestamp - really java.util.Date, plus a holder for fractional
        // seconds
        // long getTime() - ms since 1/1/1970, getNanos() - nanos value
        executeSQL(conn, "CREATE TABLE TIMESTAMPWITHTIMEZONEData (Field TIMESTAMP WITH TIME ZONE NOT NULL, FieldNullable TIMESTAMP WITH TIME ZONE);");
        executeSQL(conn, "INSERT INTO TIMESTAMPWITHTIMEZONEData (Field, FieldNullable) VALUES (TIMESTAMP '2010-01-01 00:00:00-1:00', NULL);");
        executeSQL(conn, "INSERT INTO TIMESTAMPWITHTIMEZONEData (Field, FieldNullable) VALUES (TIMESTAMP '2010-01-01 00:00:00+2:00', TIMESTAMP '2010-01-01 00:00:00-6:00');");
    }

    @Test
    public void testTIMESTAMPWITHTIMEZONE() throws Exception {
        // this should really include nanos
        List<Object> result = runMetaTest("SELECT Field, EXTRACT(TIMEZONE_HOUR FROM Field) AS Hour, FieldNullable FROM TIMESTAMPWITHTIMEZONEData", false, false);
        assertEquals(6, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(0));
        assertEquals(-1, result.get(1));
        assertNull(result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(3));
        assertEquals(2, result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(5));
    }

    @Test
    public void testTIMESTAMPWITHTIMEZONE_names() throws Exception {
        // this should really include nanos
        List<Object> result = runMetaTest("SELECT Field, EXTRACT(TIMEZONE_HOUR FROM Field) AS Hour, FieldNullable FROM TIMESTAMPWITHTIMEZONEData", true, false);
        assertEquals(12, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("HOUR", result.get(2));
        assertEquals(-1, result.get(3));
        assertEquals("FIELDNULLABLE", result.get(4));
        assertNull(result.get(5));
        assertEquals("FIELD", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(7));
        assertEquals("HOUR", result.get(8));
        assertEquals(2, result.get(9));
        assertEquals("FIELDNULLABLE", result.get(10));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(11));
    }

    // operators

    @Test
    public void testCOUNT() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) FROM INTEGERData", false, false);
        assertEquals(1, result.size());
        assertEquals(2l, result.get(0));
    }

    @Test
    public void testCOUNT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) AS TheCount FROM INTEGERData", true, false);
        assertEquals(2, result.size());
        assertEquals("THECOUNT", result.get(0));
        assertEquals(2l, result.get(1));
    }

    @Test
    public void testMINonINT() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM INTEGERData", false, false);
        assertEquals(1, result.size());
        assertEquals(0, result.get(0));
    }

    @Test
    public void testMINonINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM INTEGERData", true, false);
        assertEquals(2, result.size());
        assertEquals("THEMIN", result.get(0));
        assertEquals(0, result.get(1));
    }

    @Test
    public void testMINonREAL() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM REALData", false, false);
        assertEquals(1, result.size());
        assertEquals(0.1, (double) result.get(0), 1E-5);
    }

    @Test
    public void testMINonREAL_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM REALData", true, false);
        assertEquals(2, result.size());
        assertEquals("THEMIN", result.get(0));
        assertEquals(0.1, (double) result.get(1), 1E-5);
    }

    @Test
    public void testMINonDOUBLE() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM DOUBLEData", false, false);
        assertEquals(1, result.size());
        assertEquals(0.1, (double) result.get(0), 1E-5);
    }

    @Test
    public void testMINonDOUBLE_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM DOUBLEData", true, false);
        assertEquals(2, result.size());
        assertEquals("THEMIN", result.get(0));
        assertEquals(0.1, (double) result.get(1), 1E-5);
    }

    @Test
    public void testINTERVAL() throws Exception {
        List<Object> result = runMetaTest("SELECT (MIN(Field) - 7 DAY) AS TheInterval FROM TIMESTAMPData", false, false);
        assertEquals(1, result.size());
        // remove milliseconds from return as it is nonzero
        Calendar cal = Calendar.getInstance();
        cal.setTime((java.util.Date) result.get(0));
        cal.set(Calendar.MILLISECOND, 0);
        java.util.Date actual = cal.getTime();
        assertEquals(getDate(2009, Calendar.DECEMBER, 25, 0, 0, 0, 0), actual);
    }

    private static void setupCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE CHAR30Data (Field CHAR(30) NOT NULL, FieldNullable CHAR(30));");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL);");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "');");
    }

    @Test
    public void testCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob                           ", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred                          ", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph + "                       ", result.get(3));
    }

    @Test
    public void testCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals("Bob                           ", result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals("Fred                          ", result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph + "                       ", result.get(7));
    }

    private static void setupVARCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE VARCHAR30Data (Field VARCHAR(30) NOT NULL, FieldNullable VARCHAR(30));");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL);");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "');");
    }

    @Test
    public void testVARCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testVARCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(7));
    }

    private static void setupCLOB(Connection conn) throws SQLException {
        // like VARCHAR but for large values, java.sql.Clob or java.io.Reader
        executeSQL(conn, "CREATE TABLE CLOBData (Field CLOB NOT NULL, FieldNullable CLOB);");
        executeSQL(conn, "INSERT INTO CLOBData (Field, FieldNullable) VALUES ('Bob', NULL);");
        executeSQL(conn, "INSERT INTO CLOBData (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "');");
        // use setCharacterStream() instead?
    }

    @Test
    public void testCLOB() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CLOBData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testCLOB_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CLOBData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(7));
    }

}
