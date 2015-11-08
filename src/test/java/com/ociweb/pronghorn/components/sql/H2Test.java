package com.ociweb.pronghorn.components.sql;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.executeSQL;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.deleteFile;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.runTest;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.getDate;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.unicodeTwoHeartsGlyph;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.metaFROM;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil;
import com.ociweb.pronghorn.components.sql.DBUtil.DBUtil;
import com.ociweb.pronghorn.components.sql.DBUtil.MetaDumper;
import com.ociweb.pronghorn.components.sql.DBUtil.UserDumper;
import com.ociweb.pronghorn.components.sql.H2Component.H2Stage;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class H2Test {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(H2Test.class);

    private static String h2DB = "H2Test";
    private static String h2DBFile = "H2Test.mv.db";

    private static FieldReferenceOffsetManager userFROM = DBUtil.buildFROM("/userTemplate.xml");

    // http://www.h2database.com/html/quickstart.html
    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        // Class.forName("org.h2.Driver");
        return DriverManager.getConnection("jdbc:h2:./" + h2DB);
    }
    

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        deleteFile(h2DBFile);

        // create the database and table
        Connection conn = getConnection();
        conn.setAutoCommit(false);

        // http://www.h2database.com/html/datatypes.html
        setupINT(conn);
        setupBOOLEAN(conn);
        setupTINYINT(conn);
        setupSMALLINT(conn);
        setupBIGINT(conn);
        setupIDENTITY(conn);
        setupDECIMAL(conn);
        setupDOUBLE(conn);
        setupREAL(conn);
        setupTIME(conn);
        setupDATE(conn);
        setupTIMESTAMP(conn);
        setupBINARY(conn);
        setupOTHER(conn);
        setupVARCHAR(conn);
        setupVARCHARIGNORECASE(conn);
        setupBLOB(conn);
        setupCLOB(conn);
        setupUUID(conn);
        setupARRAY(conn);

        conn.commit();
        conn.close();
    }


    private List<Object> runMetaTest(String sql, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + sql + "' " + emitFieldNames + " " + emitRowMarkers);
        Connection conn = getConnection();
        try {
            Pipe output = new Pipe(new PipeConfig((byte) 10, (byte) 24, null, new MessageSchemaDynamic(metaFROM)));
            GraphManager gm = new GraphManager();
            H2Stage stage = new H2Stage(conn, sql, emitFieldNames, emitRowMarkers, output);
            MetaDumper dumper = new MetaDumper(gm, output);
            return runTest(stage, dumper);
        } finally {
            conn.close();
        }
    }
    
    private List<Object> runMetaTest(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + stmt.toString() + "' " + emitFieldNames + " " + emitRowMarkers);
        Pipe output = new Pipe(new PipeConfig((byte) 10, (byte) 24, null, new MessageSchemaDynamic(metaFROM)));
        GraphManager gm = new GraphManager();
        H2Stage stage = new H2Stage(stmt, emitFieldNames, emitRowMarkers, output);
        MetaDumper dumper = new MetaDumper(gm, output);
        return runTest(stage, dumper);
    }

    private List<Object> runUserTest(String sql, String message, UserDumper.Decoder decoder) throws Exception {
        // System.out.println("runUserTest(): '" + sql + "' " + message);
        Connection conn = getConnection();
        try {
            Pipe output = new Pipe(new PipeConfig((byte) 10, (byte) 24, null, new MessageSchemaDynamic(userFROM)));
            GraphManager gm = new GraphManager();
            H2Stage stage = new H2Stage(conn, sql, message, userFROM, output);
            UserDumper dumper = new UserDumper(gm, output, decoder);
            return runTest(stage, dumper);
        } finally {
            conn.close();
        }
    }

    private static void setupINT(Connection conn) throws SQLException {
        // -2147483648 to 2147483647, java.lang.Integer
        executeSQL(conn, "CREATE TABLE INTData (Field INT NOT NULL, FieldNullable INT);");
        executeSQL(conn, "INSERT INTO INTData (Field, FieldNullable) VALUES (0, NULL);");
        executeSQL(conn, "INSERT INTO INTData (Field, FieldNullable) VALUES (1, 2);");
    }

    @Test
    public void testINT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTData", false, false);
        assertEquals(4, result.size());
        assertEquals(0, result.get(0));
        assertNull(result.get(1));
        assertEquals(1, result.get(2));
        assertEquals(2, result.get(3));
    }

    @Test
    public void testINT_rowmarkers() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTData", false, true);
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
    public void testINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTData", true, false);
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
    public void testINT_names_rowmarkers() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTData", true, true);
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
    public void testINT_prepared() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;        
        try {
            conn = getConnection();
            stmt = conn.prepareStatement("SELECT Field, FieldNullable FROM INTData");
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
    
    
    class INTDecoder implements UserDumper.Decoder {
        public boolean decode(Pipe ring, int templateID, List<Object> output) throws Exception {
            FieldReferenceOffsetManager FROM = Pipe.from(ring);
            final int MSG_LOC = lookupTemplateLocator("INTData", FROM);
            final int FIELD_LOC = lookupFieldLocator("FIELD", MSG_LOC, FROM);
            final int FIELDNULLABLE_ISNULL_LOC = lookupFieldLocator("FIELDNULLABLE_IsNull", MSG_LOC, FROM);
            final int FIELDNULLABLE_LOC = lookupFieldLocator("FIELDNULLABLE", MSG_LOC, FROM);

            switch (templateID) {
            case 1:
                int v = PipeReader.readInt(ring, FIELD_LOC);
                logger.info("read FIELD, adding FIELD: " + v);
                output.add(v);
                v = PipeReader.readInt(ring, FIELDNULLABLE_ISNULL_LOC);
                logger.info("read FIELDNULLABLE_ISNULL: " + v);
                boolean FIELDNULLABLE_isNull = (v != 0);
                v = PipeReader.readInt(ring, FIELDNULLABLE_LOC);
                logger.info("read FIELDNULLABLE: " + v);
                if (FIELDNULLABLE_isNull) {
                    logger.info("adding FIELDNULLABLE null");
                    output.add(null);
                } else {
                    logger.info("adding FIELDNULLABLE not null: " + v);
                    output.add(v);
                }
                return true;
            default:
                return false;
            }
        }
    }

    @Ignore
    @Test
    public void testINT_user() throws Exception {
        // something isn't right with this - as if the reads are out of synch with the writes
        List<Object> result = runUserTest("SELECT Field, FieldNullable FROM INTData", "INTData", new INTDecoder());
        assertEquals(4, result.size());
        assertEquals(0, result.get(0));
        assertNull(result.get(1));
        assertEquals(1, result.get(2));
        assertEquals(2, result.get(3));
    }

    private static void setupBOOLEAN(Connection conn) throws SQLException {
        // TRUE and FALSE, java.lang.Boolean
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
        // -128 to 127, java.lang.Byte
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
        // -32768 to 32767, java.lang.Short
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
        // -9223372036854775808 to 9223372036854775807, java.lang.Long
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

    private static void setupIDENTITY(Connection conn) throws SQLException {
        // -9223372036854775808 to 9223372036854775807, java.lang.Long
        executeSQL(conn, "CREATE TABLE IDENTITYData (Field IDENTITY NOT NULL, FieldNullable INT);");
        executeSQL(conn, "INSERT INTO IDENTITYData (FieldNullable) VALUES (NULL);");
        executeSQL(conn, "INSERT INTO IDENTITYData (FieldNullable) VALUES (1);");
    }

    @Test
    public void testIDENTITY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM IDENTITYData", false, false);
        assertEquals(4, result.size());
        assertEquals(1l, result.get(0));
        assertNull(result.get(1));
        assertEquals(2l, result.get(2));
        assertEquals(1, result.get(3));
    }

    @Test
    public void testIDENTITY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM IDENTITYData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(1l, result.get(1));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals(2l, result.get(5));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals(1, result.get(7));
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
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMESTAMPData", false, false);
        assertEquals(10, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(0));
        assertEquals(123400000, result.get(1));
        assertEquals(0, result.get(2));
        assertNull(result.get(3));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(4));
        assertEquals(234500000, result.get(5));
        assertEquals(0, result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(7));
        assertEquals(345600000, result.get(8));
        assertEquals(0, result.get(9));
    }

    @Test
    public void testTIMESTAMP_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TIMESTAMPData", true, false);
        assertEquals(14, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(1));
        assertEquals(123400000, result.get(2));
        assertEquals(0, result.get(3));
        assertEquals("FIELDNULLABLE", result.get(4));
        assertNull(result.get(5));
        assertEquals("FIELD", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(7));
        assertEquals(234500000, result.get(8));
        assertEquals(0, result.get(9));
        assertEquals("FIELDNULLABLE", result.get(10));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(11));
        assertEquals(345600000, result.get(12));
        assertEquals(0, result.get(13));
    }

    private static void setupBINARY(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE BINARYData (Field BINARY NOT NULL, FieldNullable BINARY);");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO BINARYData (Field, FieldNullable) values (?, ?);");
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
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BINARYData", false, false);
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
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BINARYData", true, false);
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

    private static void setupVARCHAR(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE VARCHARData (Field VARCHAR NOT NULL, FieldNullable VARCHAR);");
        executeSQL(conn, "INSERT INTO VARCHARData (Field, FieldNullable) VALUES ('Bob', NULL);");
        executeSQL(conn, "INSERT INTO VARCHARData (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "');");
    }

    @Test
    public void testVARCHAR() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHARData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testVARCHAR_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHARData", true, false);
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

    private static void setupVARCHARIGNORECASE(Connection conn) throws SQLException {
        // same as VARCHAR but not case sensitive when comparing,
        // java.lang.String
        executeSQL(conn, "CREATE TABLE VARCHARIGNORECASEData (Field VARCHAR_IGNORECASE NOT NULL, FieldNullable VARCHAR_IGNORECASE)");
        executeSQL(conn, "INSERT INTO VARCHARIGNORECASEData (Field, FieldNullable) VALUES ('Bob', NULL);");
        executeSQL(conn, "INSERT INTO VARCHARIGNORECASEData (Field, FieldNullable) VALUES ('Fred', 'Alice" + unicodeTwoHeartsGlyph + "');");
    }

    @Test
    public void testVARCHARIGNORECASE() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHARIGNORECASEData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testVARCHARIGNORECASE_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHARIGNORECASEData", true, false);
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

    private static void setupUUID(Connection conn) throws SQLException {
        // universally unique identifier (128 bits), ResultSet.getObject()
        // returns a java.util.UUID, but SQL type BINARY
        executeSQL(conn, "CREATE TABLE UUIDData (Field UUID NOT NULL, FieldNullable UUID);");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO UUIDData (Field, FieldNullable) values (?, ?);");
        // UUID uuid = UUID.randomUUID();
        UUID uuid1 = UUID.fromString("33739814-9c41-48c6-9371-d027a03efd4a");
        ps.setObject(1, uuid1);
        ps.setNull(2, Types.JAVA_OBJECT);
        ps.execute();
        UUID uuid2 = UUID.fromString("33739814-9c41-48c6-9371-d027a03efd4b");
        UUID uuid3 = UUID.fromString("33739814-9c41-48c6-9371-d027a03efd4c");
        ps.setObject(1, uuid2);
        ps.setObject(2, uuid3);
        ps.execute();
        ps.close();
    }

    private String uuidBytesToString(Object value) {
        if (value == null)
            return null;

        byte[] bytes = (byte[]) value;
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong).toString();
    }

    @Test
    public void testUUID() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM UUIDData", false, false);
        assertEquals(4, result.size());
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4a", uuidBytesToString(result.get(0)));
        assertNull(result.get(1));
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4b", uuidBytesToString(result.get(2)));
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4c", uuidBytesToString(result.get(3)));
    }

    @Test
    public void testUUID_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM UUIDData", true, false);
        assertEquals(8, result.size());
        assertEquals("FIELD", result.get(0));
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4a", uuidBytesToString(result.get(1)));
        assertEquals("FIELDNULLABLE", result.get(2));
        assertNull(result.get(3));
        assertEquals("FIELD", result.get(4));
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4b", uuidBytesToString(result.get(5)));
        assertEquals("FIELDNULLABLE", result.get(6));
        assertEquals("33739814-9c41-48c6-9371-d027a03efd4c", uuidBytesToString(result.get(7)));
    }

    private static void setupARRAY(Connection conn) throws SQLException {
        // array of values, java.lang.Object[]
        executeSQL(conn, "CREATE TABLE ARRAYData (Field ARRAY NOT NULL, FieldNullable ARRAY);");
        executeSQL(conn, "INSERT INTO ARRAYData (Field, FieldNullable) VALUES ((1,2), NULL);");
        executeSQL(conn, "INSERT INTO ARRAYData (Field, FieldNullable) VALUES ((3,4), (5,6));");
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

    /*
     * In addition to direct SELECTs of all table fields, demonstrate functions (COUNT, AVG, etc.) as they produce their
     * own schema (e.g., COUNT -> long) http://www.h2database.com/html/functions.html
     */

    @Test
    public void testCOUNT() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) FROM INTData", false, false);
        assertEquals(1, result.size());
        assertEquals(2l, result.get(0));
    }

    @Test
    public void testCOUNT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) AS TheCount FROM INTData", true, false);
        assertEquals(2, result.size());
        assertEquals("THECOUNT", result.get(0));
        assertEquals(2l, result.get(1));
    }

    @Test
    public void testMINonINT() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM INTData", false, false);
        assertEquals(1, result.size());
        assertEquals(0, result.get(0));
    }

    @Test
    public void testMINonINT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM INTData", true, false);
        assertEquals(2, result.size());
        assertEquals("THEMIN", result.get(0));
        assertEquals(0, result.get(1));
    }

    @Test
    public void testMINonREAL() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM REALData", false, false);
        assertEquals(1, result.size());
        assertEquals(0.1f, (float) result.get(0), 1E-5);
    }

    @Test
    public void testMINonREAL_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM REALData", true, false);
        assertEquals(2, result.size());
        assertEquals("THEMIN", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
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
}
