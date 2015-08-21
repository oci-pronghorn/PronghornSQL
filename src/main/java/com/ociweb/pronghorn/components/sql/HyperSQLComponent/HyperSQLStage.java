package com.ociweb.pronghorn.components.sql.HyperSQLComponent;

import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeBeginGroupMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeBooleanMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeByteArrayMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDateTimeMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDecimalMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDoubleMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeEndGroupMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeFloatMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeIntMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeLongMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeSerializedMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeUTF8Message;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;

import org.apache.commons.lang3.NotImplementedException;

import com.ociweb.pronghorn.components.sql.DBUtil.Stmt;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class HyperSQLStage implements Runnable {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HyperSQLStage.class);
    private RingBuffer ring = null;
    private boolean emitFieldNames = false;
    private boolean emitRowMarkers = false;
    private Stmt stmt = null;
    private boolean useMetaMessages = false;
    private String message = null;
    private FieldReferenceOffsetManager FROM = null;

    public HyperSQLStage(Connection conn, String sql, boolean emitFieldNames, boolean emitRowMarkers, RingBuffer ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
    }

    public HyperSQLStage(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers, RingBuffer ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(stmt);
        this.ring = ring;
    }

    public HyperSQLStage(Connection conn, String sql, String message, FieldReferenceOffsetManager FROM, RingBuffer ring) throws SQLException {
        this.useMetaMessages = false;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
        this.message = message;
        this.FROM = FROM;
    }

    public HyperSQLStage(PreparedStatement stmt, String message, FieldReferenceOffsetManager FROM, RingBuffer ring) throws SQLException {
        this.useMetaMessages = false;
        this.stmt = new Stmt(stmt);
        this.ring = ring;
        this.message = message;
        this.FROM = FROM;
    }

    public void close() {
        try {
            if (stmt != null) {
                stmt.close();
                stmt = null;
            }
        } catch (SQLException e) {
        }
    }

    @Override
    protected void finalize() {
        close();
    }

    @Override
    public void run() {
        try {
            if (useMetaMessages) {
                runMetaMessages();
            }
            else {
                runFROM();
            }
        } catch (Exception e) {
            logger.error("HyperSQLStage.run(): " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    public void runMetaMessages() throws SQLException, IOException {
        ResultSet rs = stmt.getStatement().executeQuery();
        ResultSetMetaData metadata = stmt.getMetadata();

        while (rs.next()) {
            if (emitRowMarkers)
                writeBeginGroupMessage(ring, null);

            // should metadata be cached instead of referenced on each loop iteration?
            for (int col = 1; col <= metadata.getColumnCount(); col++) {
                String columnName = emitFieldNames ? metadata.getColumnName(col) : null;
                // if nullability can't be determined, assume it is nullable
                boolean isNullable = metadata.isNullable(col) != ResultSetMetaData.columnNoNulls;
                boolean isSigned = metadata.isSigned(col);

                Object value = null;
                try {
                    value = rs.getObject(col);
                } catch (SQLSyntaxErrorException e) {
                    // try to handle org.hsqldb.types.BinaryData as getObject() causes an internal exception
                    String ccn = metadata.getColumnClassName(col);
                    if (ccn == "[B")
                        value = rs.getBytes(col);
                }

                switch (metadata.getColumnType(col)) {
                case Types.BIT: // -7
                {
                    int length = metadata.getPrecision(col);
                    if (length == 1)
                        writeBooleanMessage(ring, isNullable, columnName, value);
                    else if (value instanceof org.hsqldb.types.BinaryData) {
                        // need this test as BIT2 is byte[], BIT VARYING(2) is BinaryData, yet both are Types.BIT
                        org.hsqldb.types.BinaryData data = (org.hsqldb.types.BinaryData) value;
                        writeByteArrayMessage(ring, isNullable, columnName, data.getBytes());
                    } else
                        writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                }
                case Types.TINYINT: // -6
                    writeIntMessage(ring, isNullable, isSigned, columnName, value);
                    break;
                case Types.SMALLINT: // 5
                    writeIntMessage(ring, isNullable, isSigned, columnName, value);
                    break;
                case Types.INTEGER: // 4
                    writeIntMessage(ring, isNullable, isSigned, columnName, value);
                    break;
                case Types.BIGINT: // -5
                    writeLongMessage(ring, isNullable, isSigned, columnName, value);
                    break;
                case Types.REAL: // 7
                    writeFloatMessage(ring, isNullable, columnName, value);
                    break;
                case Types.DOUBLE: // 8
                    writeDoubleMessage(ring, isNullable, columnName, value);
                    break;
                case Types.DECIMAL: // 3
                    writeDecimalMessage(ring, isNullable, columnName, value);
                    break;
                case Types.VARCHAR: // 12
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.CLOB: // 2005
                {
                    org.hsqldb.jdbc.JDBCClobClient clob = (org.hsqldb.jdbc.JDBCClobClient) value;
                    String s = (clob == null) ? null : clob.getSubString(1l, (int) clob.length());
                    writeUTF8Message(ring, isNullable, columnName, s);
                    break;
                }
                case Types.CHAR: // 1
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.DATE: // 91
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIME: // 92
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIMESTAMP: // 93
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.BINARY: // -2
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.VARBINARY: // -3
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.OTHER: // 1111
                    writeSerializedMessage(ring, isNullable, columnName, value);
                    break;
                case Types.ARRAY: // 2003
                    if (value != null)
                        value = ((org.hsqldb.jdbc.JDBCArray) value).getArray(); // the inner array is the data
                    writeSerializedMessage(ring, isNullable, columnName, value);
                    break;
                case Types.BLOB: // 2004
                {
                    // should write this in small pieces, instead of reading the entire blob at one time
                    Blob blob = (Blob) value;
                    byte[] bytes = (value == null) ? null : blob.getBytes(1l, (int) blob.length());
                    writeByteArrayMessage(ring, isNullable, columnName, bytes);
                    break;
                }
                case Types.BOOLEAN: // 16
                    writeBooleanMessage(ring, isNullable, columnName, value);
                    break;
                default:
                    throw new NotImplementedException(metadata.getColumnTypeName(col) + "/" + metadata.getColumnType(col) + " not yet implemented for column " + col + "/"
                            + metadata.getColumnName(col));
                } // switch on column type
            } // column loop

            if (emitRowMarkers)
                writeEndGroupMessage(ring);

        } // while rs.next()
    }

    public void runFROM() {
        throw new NotImplementedException("User messages are not implemented yet");
    }
}
