package com.ociweb.pronghorn.components.sql.DerbyComponent;

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
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeShortMessage;
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
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;

public class DerbyStage implements Runnable {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DerbyStage.class);
    private Pipe ring = null;
    private boolean emitFieldNames = false;
    private boolean emitRowMarkers = false;
    private Stmt stmt = null;
    private boolean useMetaMessages = false;
    private String message = null;
    private FieldReferenceOffsetManager FROM = null;

    public DerbyStage(Connection conn, String sql, boolean emitFieldNames, boolean emitRowMarkers, Pipe ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
    }

    public DerbyStage(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers, Pipe ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(stmt);
        this.ring = ring;
    }

    public DerbyStage(Connection conn, String sql, String message, FieldReferenceOffsetManager FROM, Pipe ring) throws SQLException {
        this.useMetaMessages = false;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
        this.message = message;
        this.FROM = FROM;
    }

    public DerbyStage(PreparedStatement stmt, String message, FieldReferenceOffsetManager FROM, Pipe ring) throws SQLException {
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

            for (int col = 1; col <= metadata.getColumnCount(); col++) {

                String columnName = emitFieldNames ? metadata.getColumnName(col) : null;
                // if nullability can't be determined, assume it is nullable
                boolean isNullable = metadata.isNullable(col) != ResultSetMetaData.columnNoNulls;
                // string length, binary data length, max numeric precision
                // int precision = metadata.getPrecision(col);
                // digits to the right of the decimal point, or 0 if not applicable
                // int scale = metadata.getScale(col);
                // if true, and query is ordered on the autoincrement field,
                // can set the operator to delta (pick an appropriate message)
                // boolean isAutoIncrement = metadata.isAutoIncrement(col);
                // boolean isCaseSensitive = metadata.isCaseSensitive(col);
                boolean isSigned = metadata.isSigned(col);

                // System.out.println("JDBCStage.run(): name=" + columnName + "  type=" +
                // metadata.getColumnType(col) + "  typeName=" + metadata.getColumnTypeName(col) + "  isNullable="
                // + metadata.isNullable(col) + "  isSigned=" + isSigned);

                Object value = null;
                try {
                    value = rs.getObject(col);
                } catch (SQLSyntaxErrorException e) {
                    // try to handle org.hsqldb.types.BinaryData as getObject() causes an internal exception
                    String ccn = metadata.getColumnClassName(col);
                    if (ccn == "[B")
                        value = rs.getBytes(col);
                }

                // how to send column name? want to make it a constant, but does that mean a new message?

                switch (metadata.getColumnType(col)) {
                case Types.SMALLINT: // 5
                    if (value instanceof Short)
                        writeShortMessage(ring, isNullable, isSigned, columnName, value);
                    else
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
                case Types.CHAR: // 1
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.VARCHAR: // 12
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.LONGVARCHAR: // -1
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
                case Types.LONGVARBINARY: // -4
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.JAVA_OBJECT: // 2000
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
                case Types.CLOB: // 2005
                {
                    // should really stream, but for now
                    java.sql.Clob clob = (java.sql.Clob) value;
                    String s = (value == null) ? null : clob.getSubString(1l, (int) clob.length());
                    writeUTF8Message(ring, isNullable, columnName, s);
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
