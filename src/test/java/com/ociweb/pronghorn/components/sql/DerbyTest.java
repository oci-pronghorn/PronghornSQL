package com.ociweb.pronghorn.components.sql;

import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.deleteDirectory;
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
import java.sql.Types;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ociweb.pronghorn.components.sql.DBUtil.MetaDumper;
import com.ociweb.pronghorn.components.sql.DerbyComponent.DerbyStage;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DerbyTest {
    private static String derbyDB = "derbyTest";

    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        return getConnection(false);
    }

    // http://www.h2database.com/html/quickstart.html
    private static Connection getConnection(boolean create) throws SQLException, ClassNotFoundException {
        String url = "jdbc:derby:" + derbyDB;
        if (create)
            url += ";create=true";
        return DriverManager.getConnection(url);
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        deleteDirectory(derbyDB);

        // create the database and table
        Connection conn = getConnection(true);
        conn.setAutoCommit(false);

        // http://db.apache.org/derby/docs/10.9/ref/crefsqlj31068.html
        setupINTEGER(conn);
        setupSMALLINT(conn);
        setupBIGINT(conn);
        setupREAL(conn);
        setupDOUBLE(conn);
        setupBOOLEAN(conn);
        setupBLOB(conn);
        setupDECIMAL(conn);
        setupTIME(conn);
        setupDATE(conn);
        setupTIMESTAMP(conn);
        setupCLOB(conn);
        setupVARCHAR30(conn);
        setupLONGVARCHAR(conn);
        setupCHAR30(conn);
        setupCHAR10FORBITDATA(conn);
        setupVARCHAR10FORBITDATA(conn);
        setupLONGVARCHARFORBITDATA(conn);
        setupUSERDEFINED(conn);
        setupXML(conn);

        conn.commit();
        conn.close();
    }

    private List<Object> runMetaTest(String sql, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + sql + "' " + emitFieldNames + " " + emitRowMarkers);
        Connection conn = getConnection();
        try {
            GraphManager gm = new GraphManager();
            RingBuffer output = new RingBuffer(new RingBufferConfig((byte) 10, (byte) 24, null, metaFROM));
            DerbyStage stage = new DerbyStage(conn, sql, emitFieldNames, emitRowMarkers, output);
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
        DerbyStage stage = new DerbyStage(stmt, emitFieldNames, emitRowMarkers, output);
        MetaDumper dumper = new MetaDumper(gm, output);
        return runTest(stage, dumper);
    }

    private static void setupINTEGER(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE INTEGERData (Field INTEGER NOT NULL, FieldNullable INTEGER)");
        executeSQL(conn, "INSERT INTO INTEGERData (Field, FieldNullable) VALUES (0, NULL)");
        executeSQL(conn, "INSERT INTO INTEGERData (Field, FieldNullable) VALUES (1, 2)");
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

    private static void setupSMALLINT(Connection conn) throws SQLException {
        // -32768 to 32767, java.lang.Short
        executeSQL(conn, "CREATE TABLE SMALLINTData (Field SMALLINT NOT NULL, FieldNullable SMALLINT)");
        executeSQL(conn, "INSERT INTO SMALLINTData (Field, FieldNullable) VALUES (0, NULL)");
        executeSQL(conn, "INSERT INTO SMALLINTData (Field, FieldNullable) VALUES (1, 2)");
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
        // -9223372036854775808 to 9223372036854775807, java.lang.Long
        executeSQL(conn, "CREATE TABLE BIGINTData (Field BIGINT NOT NULL, FieldNullable BIGINT)");
        executeSQL(conn, "INSERT INTO BIGINTData (Field, FieldNullable) VALUES (0, NULL)");
        executeSQL(conn, "INSERT INTO BIGINTData (Field, FieldNullable) VALUES (1, 2)");
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

    private static void setupDOUBLE(Connection conn) throws SQLException {
        // floating point number, java.lang.Double
        executeSQL(conn, "CREATE TABLE DOUBLEData (Field DOUBLE NOT NULL, FieldNullable DOUBLE)");
        executeSQL(conn, "INSERT INTO DOUBLEData (Field, FieldNullable) VALUES (0.1, NULL)");
        executeSQL(conn, "INSERT INTO DOUBLEData (Field, FieldNullable) VALUES (0.2, 0.4)");
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
        executeSQL(conn, "CREATE TABLE REALData (Field REAL NOT NULL, FieldNullable REAL)");
        executeSQL(conn, "INSERT INTO REALData (Field, FieldNullable) VALUES (0.1, NULL)");
        executeSQL(conn, "INSERT INTO REALData (Field, FieldNullable) VALUES (0.2, 0.4)");

    }

    @Test
    public void testREAL() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM REALData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1f, (float) result.get(0), 1E-5);
        assertNull(result.get(1));
        assertEquals(0.2f, (float) result.get(2), 1E-5);
        assertEquals(0.4f, (float) result.get(3), 1E-5);
    }

    @Test
    public void testREAL_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM REALData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(0.2f, (float) result.get(5), 1E-5);
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(0.4f, (float) result.get(7), 1E-5);
    }

    private static void setupBOOLEAN(Connection conn) throws SQLException {
        // TRUE and FALSE, java.lang.Boolean
        executeSQL(conn, "CREATE TABLE BOOLEANData (Field BOOLEAN NOT NULL, FieldNullable BOOLEAN)");
        executeSQL(conn, "INSERT INTO BOOLEANData (Field, FieldNullable) VALUES (FALSE, NULL)");
        executeSQL(conn, "INSERT INTO BOOLEANData (Field, FieldNullable) VALUES (TRUE, FALSE)");
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

    private static void setupBLOB(Connection conn) throws SQLException {
        // like BINARY, but for large values such as files or images,
        // java.sql.Blob or java.io.InputStream
        executeSQL(conn, "CREATE TABLE BLOBData (Field BLOB NOT NULL, FieldNullable BLOB)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO BLOBData (Field, FieldNullable) values (?, ?)");
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

    private static void setupDECIMAL(Connection conn) throws SQLException {
        // fixed precision and scale, java.math.BigDecimal
        executeSQL(conn, "CREATE TABLE DECIMALData (Field DECIMAL(20,2) NOT NULL, FieldNullable DECIMAL(10,4))");
        executeSQL(conn, "INSERT INTO DECIMALData (Field, FieldNullable) VALUES (0.1, NULL)");
        executeSQL(conn, "INSERT INTO DECIMALData (Field, FieldNullable) VALUES (0.2, 0.4)");
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

    private static void setupTIME(Connection conn) throws SQLException {
        // hh:mm:ss, java.sql.Time - really java.util.Date
        // long getTime() - ms since 1/1/1970
        executeSQL(conn, "CREATE TABLE TIMEData (Field TIME NOT NULL, FieldNullable TIME)");
        executeSQL(conn, "INSERT INTO TIMEData (Field, FieldNullable) VALUES ('00:00:00', NULL)");
        executeSQL(conn, "INSERT INTO TIMEData (Field, FieldNullable) VALUES ('00:00:01', '00:00:02')");
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
        executeSQL(conn, "CREATE TABLE DATEData (Field DATE NOT NULL, FieldNullable DATE)");
        executeSQL(conn, "INSERT INTO DATEData (Field, FieldNullable) VALUES ('2010-01-01', NULL)");
        executeSQL(conn, "INSERT INTO DATEData (Field, FieldNullable) VALUES ('2010-01-02', '2010-01-03')");
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
        executeSQL(conn, "CREATE TABLE TIMESTAMPData (Field TIMESTAMP NOT NULL, FieldNullable TIMESTAMP)");
        executeSQL(conn, "INSERT INTO TIMESTAMPData (Field, FieldNullable) VALUES ('2010-01-01 00:00:00.1234', NULL)");
        executeSQL(conn, "INSERT INTO TIMESTAMPData (Field, FieldNullable) VALUES ('2010-01-02 00:00:00.2345', '2010-01-03 00:00:00.3456')");
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

    private static void setupCLOB(Connection conn) throws SQLException {
        // like VARCHAR but for large values, java.sql.Clob or java.io.Reader
        executeSQL(conn, "CREATE TABLE CLOBData (Field CLOB NOT NULL, FieldNullable CLOB)");
        executeSQL(conn, "INSERT INTO CLOBData (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO CLOBData (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "')");
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

    private static void setupVARCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE VARCHAR30Data (Field VARCHAR(30) NOT NULL, FieldNullable VARCHAR(30))");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "')");
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

    private static void setupLONGVARCHAR(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE LONGVARCHARData (Field LONG VARCHAR NOT NULL, FieldNullable LONG VARCHAR)");
        executeSQL(conn, "INSERT INTO LONGVARCHARData (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO LONGVARCHARData (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "')");
    }

    @Test
    public void testLONGVARCHAR() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM LONGVARCHARData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testLONGVARCHAR_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM LONGVARCHARData", true, false);
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

    private static void setupCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE CHAR30Data (Field CHAR(30) NOT NULL, FieldNullable CHAR(30))");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "')");
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

    private static void setupCHAR10FORBITDATA(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE CHAR10FORBITDATA (Field CHAR(10) FOR BIT DATA NOT NULL, FieldNullable CHAR(10) FOR BIT DATA)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO CHAR10FORBITDATA (Field, FieldNullable) values (?, ?)");
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
    public void testCHAR10FORBITDATA() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR10FORBITDATA", false, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20 };
        byte[] ba2 = { 1, 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20, 0x20 };
        byte[] ba3 = { 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20 };
        assertEquals(4, result.size());
        assertArrayEquals(ba1, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(ba2, (byte[]) result.get(2));
        assertArrayEquals(ba3, (byte[]) result.get(3));
    }

    @Test
    public void testCHAR10FORBITDATA_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR10FORBITDATA", true, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20 };
        byte[] ba2 = { 1, 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20, 0x20 };
        byte[] ba3 = { 2, 3, 4, 5, 0x20, 0x20, 0x20, 0x20, 0x20, 0x20 };
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

    private static void setupVARCHAR10FORBITDATA(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE VARCHAR10FORBITDATA (Field VARCHAR(10) FOR BIT DATA NOT NULL, FieldNullable VARCHAR(10) FOR BIT DATA)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO VARCHAR10FORBITDATA (Field, FieldNullable) values (?, ?)");
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
    public void testVARCHAR10FORBITDATA() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR10FORBITDATA", false, false);
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
    public void testVARCHAR10FORBITDATA_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR10FORBITDATA", true, false);
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

    private static void setupLONGVARCHARFORBITDATA(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE LONGVARCHARFORBITDATA (Field LONG VARCHAR FOR BIT DATA NOT NULL, FieldNullable LONG VARCHAR FOR BIT DATA)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO LONGVARCHARFORBITDATA (Field, FieldNullable) values (?, ?)");
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
    public void testLONGVARCHARFORBITDATA() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM LONGVARCHARFORBITDATA", false, false);
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
    public void testLONGVARCHARFORBITDATA_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM LONGVARCHARFORBITDATA", true, false);
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

    private static void setupUSERDEFINED(Connection conn) throws SQLException {
        // serialized Java objects (as a byte array), java.lang.Object or subclass
        executeSQL(conn, "CREATE TYPE pair EXTERNAL NAME 'org.apache.commons.lang3.tuple.Pair' LANGUAGE JAVA");
        executeSQL(conn, "CREATE TABLE USERDEFINEDData (Field pair NOT NULL, FieldNullable pair)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO USERDEFINEDData (Field, FieldNullable) values (?, ?)");
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
    public void testUSERDEFINED() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM USERDEFINEDData", false, false);
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
    public void testUSERDEFINED_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM USERDEFINEDData", true, false);
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

    private static void setupXML(Connection conn) throws SQLException {
        // serialized Java objects (as a byte array), java.lang.Object or subclass
        executeSQL(conn, "CREATE TABLE XMLData (Field XML NOT NULL, FieldNullable XML)");
        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO XMLData (Field, FieldNullable) VALUES (XMLPARSE(DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE), XMLPARSE(DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE))");
        String xml1 = "<root1/>";
        ps.setObject(1, xml1);
        ps.setNull(2, Types.CLOB);
        ps.execute();
        String xml2 = "<root2/>";
        String xml3 = "<root3/>";
        ps.setObject(1, xml2);
        ps.setObject(2, xml3);
        ps.execute();
        ps.close();
    }

    @Test
    public void testXML() throws Exception {
        List<Object> result = runMetaTest("SELECT XMLSERIALIZE(Field AS CLOB), XMLSERIALIZE(FieldNullable AS CLOB) FROM XMLData", false, false);
        String xml1 = "<root1/>";
        String xml2 = "<root2/>";
        String xml3 = "<root3/>";
        assertEquals(4, result.size());
        assertEquals(xml1, result.get(0));
        assertNull(result.get(1));
        assertEquals(xml2, result.get(2));
        assertEquals(xml3, result.get(3));
    }

    @Test
    public void testXML_names() throws Exception {
        List<Object> result = runMetaTest("SELECT XMLSERIALIZE(Field AS CLOB) AS TheField, XMLSERIALIZE(FieldNullable AS CLOB) AS TheFieldNullable FROM XMLData", true, false);
        String xml1 = "<root1/>";
        String xml2 = "<root2/>";
        String xml3 = "<root3/>";
        assertEquals(8, result.size());
        assertEquals("THEFIELD", result.get(0));
        assertEquals(xml1, result.get(1));
        assertEquals("THEFIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("THEFIELD", result.get(4));
        assertEquals(xml2, result.get(5));
        assertEquals("THEFIELDNULLABLE", result.get(6));
        assertEquals(xml3, result.get(7));
    }

}
