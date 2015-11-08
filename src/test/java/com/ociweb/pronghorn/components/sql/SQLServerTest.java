package com.ociweb.pronghorn.components.sql;

import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.executeSQL;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.getDate;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.metaFROM;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.parseConfig;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.parseElement;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.runTest;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.unicodeEuroGlyph;
import static com.ociweb.pronghorn.components.sql.DBUtil.DBTestUtil.unicodeTwoHeartsGlyph;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Element;

import com.ociweb.pronghorn.components.sql.DBUtil.DBUtil;
import com.ociweb.pronghorn.components.sql.DBUtil.MetaDumper;
import com.ociweb.pronghorn.components.sql.DBUtil.UserDumper;
import com.ociweb.pronghorn.components.sql.SQLServerComponent.SQLServerStage;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SQLServerTest {
    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SQLServerTest.class);
    private static FieldReferenceOffsetManager userFROM = DBUtil.buildFROM("/userTemplate.xml");

    private static String dbServer = "//localhost\\SS2012";
    private static String dbAdminUser = "sa";
    private static String dbAdminPassword = "sa123";
    private static String dbDatabase = "PronghornTest";
    private static String dbUser = "PronghornUser";
    private static String dbPassword = "PronghornPwd123!";
    private static String dbCreateExt = "";
    private static SQLServerVersion version = null;

    // http://msdn.microsoft.com/en-us/library/ms378526%28v=sql.110%29.aspx
    private static Connection getConnection(String server, String database, String user, String password) throws SQLException, ClassNotFoundException {
        // Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return DriverManager.getConnection("jdbc:sqlserver:" + server + ";databaseName=" + database + ";user=" + user + ";password=" + password + ";");
    }

    // connect to the test database with normal user rights
    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        return getConnection(dbServer, dbDatabase, dbUser, dbPassword);
    }

    // connect to the test database with admin rights
    private static Connection getAdminConnection() throws SQLException, ClassNotFoundException {
        return getConnection(dbServer, dbDatabase, dbAdminUser, dbAdminPassword);
    }

    // connect to the master database with admin rights
    private static Connection getMasterConnection() throws SQLException, ClassNotFoundException {
        return getConnection(dbServer, "master", dbAdminUser, dbAdminPassword);
    }

    // get the product version of the SQL Server instance
    private static SQLServerVersion getServerVersion(Connection conn) throws SQLException {
        SQLServerVersion version = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR) AS ProductVersion, CAST(SERVERPROPERTY('productlevel') AS VARCHAR) AS ProductLevel, "
                    + "CAST(SERVERPROPERTY('edition') AS VARCHAR) AS Edition");
            if (rs.next()) {
                String[] productVersion = rs.getString("ProductVersion").split("\\.");
                version = new SQLServerVersion(Integer.parseInt(productVersion[0]), Integer.parseInt(productVersion[1]), Integer.parseInt(productVersion[2]), rs.getString("ProductLevel"),
                        rs.getString("Edition"));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }

        return version;
    }

    // create a user at the level of the database server
    private static void createDatabaseServerUser(Connection conn) throws SQLException {
        if (version.getMajorVersion() >= 9) {
            executeSQL(conn, "CREATE LOGIN " + dbUser + " WITH PASSWORD = '" + dbPassword + "'");
        } else {
            PreparedStatement pstmt = null;
            try {
                pstmt = conn.prepareStatement("sp_addlogin");
                pstmt.setString(1, dbUser); // @loginame
                pstmt.setString(2, dbPassword); // @passwd
                pstmt.execute();
            } finally {
                if (pstmt != null) {
                    pstmt.close();
                }
            }
        }
    }

    // return true if the database user has already been created
    private static boolean databaseServerUserExists(Connection conn) throws SQLException {
        boolean userExists = false;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT COUNT(loginname) AS UserCount FROM master.dbo.syslogins WHERE loginname = '" + dbUser + "'");
            if (rs.next()) {
                userExists = rs.getInt("UserCount") > 0;
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }

        return userExists;
    }

    // http://www.microsoft.com/technet/prodtechnol/sql/2000/books/c05ppcsq.mspx
    private static void createUser() throws SQLException, ClassNotFoundException {
        // open the target database using the server admin credentials as need full control
        Connection conn = null;
        try {
            conn = getAdminConnection();
            conn.setAutoCommit(false);
            if (!databaseServerUserExists(conn)) {
                createDatabaseServerUser(conn);
            }
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static void createDatabase() throws SQLException, ClassNotFoundException {
        Connection conn = null;

        try {
            conn = getMasterConnection();
            version = getServerVersion(conn);

            executeSQL(conn, "CREATE DATABASE [" + dbDatabase + "] " + dbCreateExt);

            try {
                executeSQL(conn, "ALTER DATABASE [" + dbDatabase + "] SET RECOVERY SIMPLE");
            } catch (Exception e) {
                // nonfatal
                logger.error("Cannot alter database", e);
            }

            createUser();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

        conn = getAdminConnection();
        try {
            if (version.getMajorVersion() > 9) {
                executeSQL(conn, "CREATE USER " + dbUser + " FOR LOGIN " + dbUser);

                // give the user the db_owner role
                PreparedStatement pstmt = null;
                try {
                    pstmt = conn.prepareStatement("exec sp_addrolemember ?,?");
                    pstmt.setString(1, "db_owner"); // @rolename
                    pstmt.setString(2, dbUser); // @membername
                    pstmt.execute();
                } finally {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                }
            } else {
                // change the owner of the database from adminUser to user, so user becomes dbo
                PreparedStatement pstmt = null;
                try {
                    pstmt = conn.prepareStatement("exec sp_changedbowner ?");
                    pstmt.setString(1, dbUser); // @loginame
                    pstmt.execute();
                } finally {
                    if (pstmt != null) {
                        pstmt.close();
                    }
                }
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    private static void dropDatabase() throws SQLException, ClassNotFoundException {
        Connection conn = null;
        try {
            conn = getMasterConnection();
            executeSQL(conn, "IF EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE " + "name = N'" + dbDatabase + "') DROP DATABASE [" + dbDatabase + "]");
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
    
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Element config = parseConfig();
        if (config != null) {
            dbServer = parseElement(config, "server", dbServer);
            dbAdminUser = parseElement(config, "adminUser", dbAdminUser);         
            dbAdminPassword = parseElement(config, "adminPassword", dbAdminPassword);       
            dbDatabase = parseElement(config, "database", dbDatabase);  
            dbUser = parseElement(config, "user", dbUser);         
            dbPassword = parseElement(config, "password", dbPassword);  
            dbCreateExt = parseElement(config, "createExt", dbCreateExt);            
        }

        dropDatabase();
        createDatabase();

        Connection conn = getConnection();
        conn.setAutoCommit(false);

        // http://msdn.microsoft.com/en-us/library/ms187752.aspx
        
        setupBIT(conn);
        setupINT(conn);
        setupTINYINT(conn);
        setupSMALLINT(conn);
        setupBIGINT(conn);
        setupFLOAT(conn);
        setupREAL(conn);
        setupTIME(conn);
        setupDATE(conn);
        setupDECIMAL(conn);
        setupNUMERIC(conn);
        setupCHAR30(conn);
        setupVARCHAR30(conn);
        setupNCHAR30(conn);
        setupNVARCHAR30(conn);
        setupBINARY10(conn);
        setupVARBINARY10(conn);
        setupMONEY(conn);
        setupSMALLMONEY(conn);
        setupDATETIME(conn);
        setupDATETIME2(conn);
        setupSMALLDATETIME(conn);
        setupDATETIMEOFFSET(conn);
        setupUNIQUEIDENTIFIER(conn);
        setupGEOMETRY(conn);
        setupGEOGRAPHY(conn);
        setupXML(conn);
        setupHIERARCHYID(conn);

        // TIMESTAMP/ROWVERSION, CURSOR - not something that can be queried
        // SQL_VARIANT - generic field for built-in types
        // TABLE - temporary row storage
        // NTEXT/TEXT/IMAGE going away - use NVARCHAR(max), VARCHAR(max), VARBINARY(max)
        setupTEXT(conn);
        setupNTEXT(conn);

        conn.commit();
        conn.close();
    }

    private List<Object> runMetaTest(String sql, boolean emitFieldNames, boolean emitRowMarkers) throws Exception {
        // System.out.println("runMetaTest(): '" + sql + "' " + emitFieldNames + " " + emitRowMarkers);
        Connection conn = getConnection();
        try {
            Pipe output = new Pipe(new PipeConfig((byte) 10, (byte) 24, null, new MessageSchemaDynamic(metaFROM)));
            GraphManager gm = new GraphManager();
            SQLServerStage stage = new SQLServerStage(conn, sql, emitFieldNames, emitRowMarkers, output);
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
        SQLServerStage stage = new SQLServerStage(stmt, emitFieldNames, emitRowMarkers, output);
        MetaDumper dumper = new MetaDumper(gm, output);
        return runTest(stage, dumper);
    }

    private static void setupINT(Connection conn) throws SQLException {
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
        assertEquals("Field", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(2, result.get(7));
    }

    @Test
    public void testINT_names_rowmarkers() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM INTData", true, true);
        assertEquals(12, result.size());
        assertEquals("**BEGIN GROUP**", result.get(0));
        assertEquals("Field", result.get(1));
        assertEquals(0, result.get(2));
        assertEquals("FieldNullable", result.get(3));
        assertNull(result.get(4));
        assertEquals("**END GROUP**", result.get(5));
        assertEquals("**BEGIN GROUP**", result.get(6));
        assertEquals("Field", result.get(7));
        assertEquals(1, result.get(8));
        assertEquals("FieldNullable", result.get(9));
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

    private static void setupBIT(Connection conn) throws SQLException {
        // TRUE and FALSE, java.lang.Boolean
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
        assertEquals("Field", result.get(0));
        assertEquals(false, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(true, result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(0, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(1, result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(0l, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(1l, result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(BigDecimal.valueOf(10, 2), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(BigDecimal.valueOf(20, 2), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(BigDecimal.valueOf(4000, 4), result.get(7));
    }

    private static void setupNUMERIC(Connection conn) throws SQLException {
        // fixed precision and scale, java.math.BigDecimal
        executeSQL(conn, "CREATE TABLE NUMERICData (Field NUMERIC(20,2) NOT NULL, FieldNullable NUMERIC(10,4));");
        executeSQL(conn, "INSERT INTO NUMERICData (Field, FieldNullable) VALUES (0.1, NULL);");
        executeSQL(conn, "INSERT INTO NUMERICData (Field, FieldNullable) VALUES (0.2, 0.4);");
    }

    @Test
    public void testNUMERIC() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NUMERICData", false, false);
        assertEquals(4, result.size());
        assertEquals(BigDecimal.valueOf(10, 2), result.get(0));
        assertNull(result.get(1));
        assertEquals(BigDecimal.valueOf(20, 2), result.get(2));
        assertEquals(BigDecimal.valueOf(4000, 4), result.get(3));
    }

    @Test
    public void testNUMERIC_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NUMERICData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(BigDecimal.valueOf(10, 2), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(BigDecimal.valueOf(20, 2), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(BigDecimal.valueOf(4000, 4), result.get(7));
    }

    private static void setupFLOAT(Connection conn) throws SQLException {
        // floating point number, java.lang.Double
        executeSQL(conn, "CREATE TABLE FLOATData (Field FLOAT NOT NULL, FieldNullable FLOAT);");
        executeSQL(conn, "INSERT INTO FLOATData (Field, FieldNullable) VALUES (0.1, NULL);");
        executeSQL(conn, "INSERT INTO FLOATData (Field, FieldNullable) VALUES (0.2, 0.4);");
    }

    @Test
    public void testFLOAT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM FLOATData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1, result.get(0));
        assertNull(result.get(1));
        assertEquals(0.2, result.get(2));
        assertEquals(0.4, result.get(3));
    }

    @Test
    public void testFLOAT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM FLOATData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(0.1, result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(0.2, result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(0.2f, (float) result.get(5), 1E-5);
        assertEquals("FieldNullable", result.get(6));
        assertEquals(0.4f, (float) result.get(7), 1E-5);
    }

    private static void setupMONEY(Connection conn) throws SQLException {
        // single precision floating point number, java.lang.Float
        executeSQL(conn, "CREATE TABLE MONEYData (Field REAL NOT NULL, FieldNullable REAL);");
        executeSQL(conn, "INSERT INTO MONEYData (Field, FieldNullable) VALUES (" + unicodeEuroGlyph + "0.1, NULL);");
        executeSQL(conn, "INSERT INTO MONEYData (Field, FieldNullable) VALUES (" + unicodeEuroGlyph + "0.2, " + unicodeEuroGlyph + "0.4);");

    }

    @Test
    public void testMONEY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM MONEYData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1f, (float) result.get(0), 1E-5);
        assertNull(result.get(1));
        assertEquals(0.2f, (float) result.get(2), 1E-5);
        assertEquals(0.4f, (float) result.get(3), 1E-5);
    }

    @Test
    public void testMONEY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM MONEYData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(0.2f, (float) result.get(5), 1E-5);
        assertEquals("FieldNullable", result.get(6));
        assertEquals(0.4f, (float) result.get(7), 1E-5);
    }

    private static void setupSMALLMONEY(Connection conn) throws SQLException {
        // single precision floating point number, java.lang.Float
        executeSQL(conn, "CREATE TABLE SMALLMONEYData (Field REAL NOT NULL, FieldNullable REAL);");
        executeSQL(conn, "INSERT INTO SMALLMONEYData (Field, FieldNullable) VALUES (" + unicodeEuroGlyph + "0.1, NULL);");
        executeSQL(conn, "INSERT INTO SMALLMONEYData (Field, FieldNullable) VALUES (" + unicodeEuroGlyph + "0.2, " + unicodeEuroGlyph + "0.4);");

    }

    @Test
    public void testSMALLMONEY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLMONEYData", false, false);
        assertEquals(4, result.size());
        assertEquals(0.1f, (float) result.get(0), 1E-5);
        assertNull(result.get(1));
        assertEquals(0.2f, (float) result.get(2), 1E-5);
        assertEquals(0.4f, (float) result.get(3), 1E-5);
    }

    @Test
    public void testSMALLMONEY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLMONEYData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(0.2f, (float) result.get(5), 1E-5);
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(getDate(1970, Calendar.JANUARY, 1, 0, 0, 1, 0), result.get(5));
        assertEquals("FieldNullable", result.get(6));
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
        assertEquals("Field", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(7));
    }

    private static void setupBINARY10(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE BINARY10Data (Field BINARY(10) NOT NULL, FieldNullable BINARY(10));");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO BINARY10Data (Field, FieldNullable) values (?, ?);");
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        ps.setBytes(1, ba1);
        ps.setNull(2, java.sql.Types.BINARY);
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
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BINARY10Data", false, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5, 0, 0, 0, 0 };
        byte[] ba2 = { 1, 2, 3, 4, 5, 0, 0, 0, 0, 0 };
        byte[] ba3 = { 2, 3, 4, 5, 0, 0, 0, 0, 0, 0 };
        assertEquals(4, result.size());
        assertArrayEquals(ba1, (byte[]) result.get(0));
        assertNull(result.get(1));
        assertArrayEquals(ba2, (byte[]) result.get(2));
        assertArrayEquals(ba3, (byte[]) result.get(3));
    }

    @Test
    public void testBINARY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM BINARY10Data", true, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5, 0, 0, 0, 0 };
        byte[] ba2 = { 1, 2, 3, 4, 5, 0, 0, 0, 0, 0 };
        byte[] ba3 = { 2, 3, 4, 5, 0, 0, 0, 0, 0, 0 };
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertArrayEquals(ba1, (byte[]) result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertArrayEquals(ba2, (byte[]) result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertArrayEquals(ba3, (byte[]) result.get(7));
    }

    private static void setupVARBINARY10(Connection conn) throws SQLException {
        // byte array, byte[]
        executeSQL(conn, "CREATE TABLE VARBINARY10Data (Field VARBINARY(10) NOT NULL, FieldNullable VARBINARY(10));");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO VARBINARY10Data (Field, FieldNullable) values (?, ?);");
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        ps.setBytes(1, ba1);
        ps.setNull(2, java.sql.Types.BINARY);
        ps.execute();
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        ps.setBytes(1, ba2);
        ps.setBytes(2, ba3);
        ps.execute();
        ps.close();
    }

    @Test
    public void testVARBINARY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARBINARY10Data", false, false);
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
    public void testVARBINARY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARBINARY10Data", true, false);
        byte[] ba1 = { 0, 1, 2, 3, 4, 5 };
        byte[] ba2 = { 1, 2, 3, 4, 5 };
        byte[] ba3 = { 2, 3, 4, 5 };
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertArrayEquals(ba1, (byte[]) result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertArrayEquals(ba2, (byte[]) result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertArrayEquals(ba3, (byte[]) result.get(7));
    }

    private static void setupVARCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE VARCHAR30Data (Field VARCHAR(30) NOT NULL, FieldNullable VARCHAR(30))");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO VARCHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice')");
    }

    @Test
    public void testVARCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice", result.get(3));
    }

    @Test
    public void testVARCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM VARCHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice", result.get(7));
    }

    private static void setupTEXT(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE TEXTData (Field TEXT NOT NULL, FieldNullable TEXT)");
        executeSQL(conn, "INSERT INTO TEXTData (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO TEXTData (Field, FieldNullable) VALUES ('Fred', 'Alice')");
    }

    @Test
    public void testTEXT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TEXTData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice", result.get(3));
    }

    @Test
    public void testTEXT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM TEXTData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice", result.get(7));
    }

    private static void setupCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE CHAR30Data (Field CHAR(30) NOT NULL, FieldNullable CHAR(30))");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Bob', NULL)");
        executeSQL(conn, "INSERT INTO CHAR30Data (Field, FieldNullable) VALUES ('Fred', 'Alice')");
    }

    @Test
    public void testCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob                           ", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred                          ", result.get(2));
        assertEquals("Alice                         ", result.get(3));
    }

    @Test
    public void testCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM CHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob                           ", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred                          ", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice                         ", result.get(7));
    }

    private static void setupNVARCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE NVARCHAR30Data (Field NVARCHAR(30) NOT NULL, FieldNullable NVARCHAR(30))");
        executeSQL(conn, "INSERT INTO NVARCHAR30Data (Field, FieldNullable) VALUES (N'Bob', NULL)");
        executeSQL(conn, "INSERT INTO NVARCHAR30Data (Field, FieldNullable) VALUES (N'Fred', N'Alice" + unicodeTwoHeartsGlyph + "')");
    }

    @Test
    public void testNVARCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NVARCHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testNVARCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NVARCHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(7));
    }

    private static void setupNTEXT(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE NTEXTData (Field NTEXT NOT NULL, FieldNullable NTEXT)");
        executeSQL(conn, "INSERT INTO NTEXTData (Field, FieldNullable) VALUES (N'Bob', NULL)");
        executeSQL(conn, "INSERT INTO NTEXTData (Field, FieldNullable) VALUES (N'Fred', N'Alice" + unicodeTwoHeartsGlyph + "')");
    }

    @Test
    public void testNTEXT() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NTEXTData", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(3));
    }

    @Test
    public void testNTEXT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NTEXTData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph, result.get(7));
    }

    private static void setupNCHAR30(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE NCHAR30Data (Field NCHAR(30) NOT NULL, FieldNullable NCHAR(30))");
        executeSQL(conn, "INSERT INTO NCHAR30Data (Field, FieldNullable) VALUES (N'Bob', NULL)");
        executeSQL(conn, "INSERT INTO NCHAR30Data (Field, FieldNullable) VALUES (N'Fred', N'Alice" + unicodeTwoHeartsGlyph + "')");
    }

    @Test
    public void testNCHAR30() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NCHAR30Data", false, false);
        assertEquals(4, result.size());
        assertEquals("Bob                           ", result.get(0));
        assertNull(result.get(1));
        assertEquals("Fred                          ", result.get(2));
        assertEquals("Alice" + unicodeTwoHeartsGlyph + "                       ", result.get(3));
    }

    @Test
    public void testNCHAR30_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM NCHAR30Data", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("Bob                           ", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("Fred                          ", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("Alice" + unicodeTwoHeartsGlyph + "                       ", result.get(7));
    }
    
    private static void setupDATETIME(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE DATETIMEData (Field DATETIME NOT NULL, FieldNullable DATETIME);");
        executeSQL(conn, "INSERT INTO DATETIMEData (Field, FieldNullable) VALUES ('2010-01-01 00:00:00', NULL);");
        executeSQL(conn, "INSERT INTO DATETIMEData (Field, FieldNullable) VALUES ('2010-01-02 00:00:00', '2010-01-03 00:00:00');");
    }

    @Test
    public void testDATETIME() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIMEData", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(3));
    }

    @Test
    public void testDATETIME_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIMEData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(7));
    }
    
    private static void setupDATETIME2(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE DATETIME2Data (Field DATETIME2 NOT NULL, FieldNullable DATETIME2);");
        executeSQL(conn, "INSERT INTO DATETIME2Data (Field, FieldNullable) VALUES ('2010-01-01 00:00:00.123', NULL);");
        executeSQL(conn, "INSERT INTO DATETIME2Data (Field, FieldNullable) VALUES ('2010-01-02 00:00:00.234', '2010-01-03 00:00:00.345');");
    }

    @Test
    public void testDATETIME2() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIME2Data", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(3));
    }

    @Test
    public void testDATETIME2_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIME2Data", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 123), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 234), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 345), result.get(7));
    }
    
    private static void setupSMALLDATETIME(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE SMALLDATETIMEData (Field SMALLDATETIME NOT NULL, FieldNullable SMALLDATETIME);");
        executeSQL(conn, "INSERT INTO SMALLDATETIMEData (Field, FieldNullable) VALUES ('2010-01-01 00:00:00', NULL);");
        executeSQL(conn, "INSERT INTO SMALLDATETIMEData (Field, FieldNullable) VALUES ('2010-01-02 00:00:00', '2010-01-03 00:00:00');");
    }

    @Test
    public void testSMALLDATETIME() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLDATETIMEData", false, false);
        assertEquals(4, result.size());
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(0));
        assertNull(result.get(1));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(2));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(3));
    }

    @Test
    public void testSMALLDATETIME_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM SMALLDATETIMEData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 0, 0, 0, 0), result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 0, 0, 0, 0), result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 3, 0, 0, 0, 0), result.get(7));
    }

    
    private static void setupDATETIMEOFFSET(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE DATETIMEOFFSETData (Field DATETIMEOFFSET NOT NULL, FieldNullable DATETIMEOFFSET);");
        executeSQL(conn, "INSERT INTO DATETIMEOFFSETData (Field, FieldNullable) VALUES ('2010-01-01 00:00:00.123456 +01:00', NULL);");
        executeSQL(conn, "INSERT INTO DATETIMEOFFSETData (Field, FieldNullable) VALUES ('2010-01-02 00:00:00.234 -02:00', '2010-01-03 00:00:00.345 +03:00');");
    }

    @Test
    public void testDATETIMEOFFSET() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIMEOFFSETData", false, false);
        assertEquals(10, result.size());
        assertEquals(getDate(2009, Calendar.DECEMBER, 31, 17, 0, 0, 123), result.get(0));
        assertEquals(123456000, result.get(1));
        assertEquals(60, result.get(2));
        assertNull(result.get(3));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 20, 0, 0, 234), result.get(4));
        assertEquals(234000000, result.get(5));
        assertEquals(-120, result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 15, 0, 0, 345), result.get(7));
        assertEquals(345000000, result.get(8));
        assertEquals(180, result.get(9));
    }

    @Test
    public void testDATETIMEOFFSET_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM DATETIMEOFFSETData", true, false);
        assertEquals(14, result.size());
        assertEquals("Field", result.get(0));
        assertEquals(getDate(2009, Calendar.DECEMBER, 31, 17, 0, 0, 123), result.get(1));
        assertEquals(123456000, result.get(2));
        assertEquals(60, result.get(3));
        assertEquals("FieldNullable", result.get(4));
        assertNull(result.get(5));
        assertEquals("Field", result.get(6));
        assertEquals(getDate(2010, Calendar.JANUARY, 1, 20, 0, 0, 234), result.get(7));
        assertEquals(234000000, result.get(8));
        assertEquals(-120, result.get(9));
        assertEquals("FieldNullable", result.get(10));
        assertEquals(getDate(2010, Calendar.JANUARY, 2, 15, 0, 0, 345), result.get(11));
        assertEquals(345000000, result.get(12));
        assertEquals(180, result.get(13));
    }
    
    
    
    
    
    
    
    
    private static void setupUNIQUEIDENTIFIER(Connection conn) throws SQLException {
        // universally unique identifier (128 bits), ResultSet.getObject()
        // returns a java.util.UUID, but SQL type BINARY
        executeSQL(conn, "CREATE TABLE UNIQUEIDENTIFIERData (Field UNIQUEIDENTIFIER NOT NULL, FieldNullable UNIQUEIDENTIFIER);");
        executeSQL(conn, "INSERT INTO UNIQUEIDENTIFIERData (Field, FieldNullable) values (N'33739814-9c41-48c6-9371-d027a03efd4a', NULL);");
        executeSQL(conn, "INSERT INTO UNIQUEIDENTIFIERData (Field, FieldNullable) values (N'33739814-9c41-48c6-9371-d027a03efd4b', N'33739814-9c41-48c6-9371-d027a03efd4c');");
    }

    @Test
    public void testUNIQUEIDENTIFIER() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM UNIQUEIDENTIFIERData", false, false);
        assertEquals(4, result.size());
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4A", result.get(0));
        assertNull(result.get(1));
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4B", result.get(2));
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4C", result.get(3));
    }

    @Test
    public void testUNIQUEIDENTIFIER_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM UNIQUEIDENTIFIERData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4A", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4B", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("33739814-9C41-48C6-9371-D027A03EFD4C", result.get(7));
    }

    
    
    private static void setupGEOMETRY(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE GEOMETRYData (Field GEOMETRY NOT NULL, FieldNullable GEOMETRY)");
        executeSQL(conn, "INSERT INTO GEOMETRYData (Field, FieldNullable) VALUES (" + 
                "geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0), NULL)");
        executeSQL(conn, "INSERT INTO GEOMETRYData (Field, FieldNullable) VALUES (" + 
                "geometry::STGeomFromText('POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))', 0), " + 
                "geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0))");
    }

    @Test
    public void testGEOMETRY() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.STAsText(), FieldNullable.STAsText() FROM GEOMETRYData", false, false);
        assertEquals(4, result.size());
        assertEquals("LINESTRING (100 100, 20 180, 180 180)", result.get(0));
        assertNull(result.get(1));
        assertEquals("POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))", result.get(2));
        assertEquals("LINESTRING (100 100, 20 180, 180 180)", result.get(3));
    }

    @Test
    public void testGEOMETRY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.STAsText() AS Field, FieldNullable.STAsText() AS FieldNullable FROM GEOMETRYData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("LINESTRING (100 100, 20 180, 180 180)", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("LINESTRING (100 100, 20 180, 180 180)", result.get(7));
    }
    
    
    
    private static void setupGEOGRAPHY(Connection conn) throws SQLException {
        executeSQL(conn, "CREATE TABLE GEOGRAPHYData (Field GEOGRAPHY NOT NULL, FieldNullable GEOGRAPHY)");
        executeSQL(conn, "INSERT INTO GEOGRAPHYData (Field, FieldNullable) VALUES (" +
                "geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326), NULL)");
        executeSQL(conn, "INSERT INTO GEOGRAPHYData (Field, FieldNullable) VALUES (" + 
                "geography::STGeomFromText('POLYGON((-122.358 47.653 , -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))', 4326)," +
                "geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326))");
                
    }

    @Test
    public void testGEOGRAPHY() throws Exception {
        // without STAsText(), a binary array is returned
        List<Object> result = runMetaTest("SELECT Field.STAsText(), FieldNullable.STAsText() FROM GEOGRAPHYData", false, false);
        assertEquals(4, result.size());
        assertEquals("LINESTRING (-122.36 47.656, -122.343 47.656)", result.get(0));
        assertNull(result.get(1));
        assertEquals("POLYGON ((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))", result.get(2));
        assertEquals("LINESTRING (-122.36 47.656, -122.343 47.656)", result.get(3));
    }

    @Test
    public void testGEOGRAPHY_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.STAsText() AS Field, FieldNullable.STAsText() AS FieldNullable FROM GEOGRAPHYData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("LINESTRING (-122.36 47.656, -122.343 47.656)", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("POLYGON ((-122.358 47.653, -122.348 47.649, -122.348 47.658, -122.358 47.658, -122.358 47.653))", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("LINESTRING (-122.36 47.656, -122.343 47.656)", result.get(7));
    }
    
    
    
    private static void setupXML(Connection conn) throws SQLException {
        // Unicode string, java.lang.String
        executeSQL(conn, "CREATE TABLE XMLData (Field XML NOT NULL, FieldNullable XML)");
        executeSQL(conn, "INSERT INTO XMLData (Field, FieldNullable) VALUES ('<root />', NULL)");
        executeSQL(conn, "INSERT INTO XMLData (Field, FieldNullable) VALUES ('<item at=\"1\" />', '<parent><child /></parent>')");
    }

    @Test
    public void testXML() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM XMLData", false, false);
        assertEquals(4, result.size());
        assertEquals("<root/>", result.get(0));
        assertNull(result.get(1));
        assertEquals("<item at=\"1\"/>", result.get(2));
        assertEquals("<parent><child/></parent>", result.get(3));
    }

    @Test
    public void testXML_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field, FieldNullable FROM XMLData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("<root/>", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("<item at=\"1\"/>", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("<parent><child/></parent>", result.get(7));
    }

    
    private static void setupHIERARCHYID(Connection conn) throws SQLException {
        // A raw HIERARCHYID is retrieved as a byte[] - use ToString() for a text representation
        executeSQL(conn, "CREATE TABLE HIERARCHYIDData (Field HIERARCHYID NOT NULL, FieldNullable HIERARCHYID)");
        executeSQL(conn, "INSERT INTO HIERARCHYIDData (Field, FieldNullable) VALUES (hierarchyid::GetRoot(), NULL)");
        executeSQL(conn, "INSERT INTO HIERARCHYIDData (Field, FieldNullable) VALUES (hierarchyid::GetRoot(), hierarchyid::GetRoot())");
    }

    @Test
    public void testHIERARCHYID() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.ToString() AS Field, FieldNullable.ToString() AS FieldNullable FROM HIERARCHYIDData", false, false);
        assertEquals(4, result.size());
        assertEquals("/", result.get(0));
        assertNull(result.get(1));
        assertEquals("/", result.get(2));
        assertEquals("/", result.get(3));
    }

    @Test
    public void testHIERARCHYID_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.ToString() AS Field, FieldNullable.ToString() AS FieldNullable FROM HIERARCHYIDData", true, false);
        assertEquals(8, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("/", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertNull(result.get(3));
        assertEquals("Field", result.get(4));
        assertEquals("/", result.get(5));
        assertEquals("FieldNullable", result.get(6));
        assertEquals("/", result.get(7));
    }
    

    // In addition to direct SELECTs of all table fields, demonstrate functions (COUNT, AVG, etc.) as they produce their
    // own schema

    @Test
    public void testCOUNT() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) FROM INTData", false, false);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0));
    }

    @Test
    public void testCOUNT_names() throws Exception {
        List<Object> result = runMetaTest("SELECT COUNT(*) AS TheCount FROM INTData", true, false);
        assertEquals(2, result.size());
        assertEquals("TheCount", result.get(0));
        assertEquals(2, result.get(1));
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
        assertEquals("TheMin", result.get(0));
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
        assertEquals("TheMin", result.get(0));
        assertEquals(0.1f, (float) result.get(1), 1E-5);
    }

    @Test
    public void testMINonDOUBLE() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) FROM FLOATData", false, false);
        assertEquals(1, result.size());
        assertEquals(0.1, (double) result.get(0), 1E-5);
    }

    @Test
    public void testMINonDOUBLE_names() throws Exception {
        List<Object> result = runMetaTest("SELECT MIN(Field) AS TheMin FROM FLOATData", true, false);
        assertEquals(2, result.size());
        assertEquals("TheMin", result.get(0));
        assertEquals(0.1, (double) result.get(1), 1E-5);
    }
    
    @Test
    public void testSTIntersection_names() throws Exception {
        List<Object> result = runMetaTest("SELECT Field.STAsText() AS Field, FieldNullable.STAsText() AS FieldNullable, Field.STIntersection(FieldNullable).STAsText() AS TheIntersection FROM GEOMETRYData WHERE FieldNullable IS NOT NULL", true, false);
        assertEquals(6, result.size());
        assertEquals("Field", result.get(0));
        assertEquals("POLYGON ((0 0, 150 0, 150 150, 0 150, 0 0))", result.get(1));
        assertEquals("FieldNullable", result.get(2));
        assertEquals("LINESTRING (100 100, 20 180, 180 180)", result.get(3));
        assertEquals("TheIntersection", result.get(4));
        assertEquals("LINESTRING (50.000000000000782 149.99999999999977, 100 100)", result.get(5));
    }
}
