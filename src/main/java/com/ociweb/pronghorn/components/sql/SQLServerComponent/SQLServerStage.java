package com.ociweb.pronghorn.components.sql.SQLServerComponent;

import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeASCIIMessage;
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
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeShortMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeTimestampMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeUTF8Message;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.lang3.NotImplementedException;

import com.ociweb.pronghorn.components.sql.DBUtil.Stmt;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;

public class SQLServerStage implements Runnable {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SQLServerStage.class);
    private Pipe ring = null;
    private boolean emitFieldNames = false;
    private boolean emitRowMarkers = false;
    private Stmt stmt = null;
    private boolean useMetaMessages = false;
    private String message = null;
    private FieldReferenceOffsetManager FROM = null;

    public SQLServerStage(Connection conn, String sql, boolean emitFieldNames, boolean emitRowMarkers, Pipe ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
    }

    public SQLServerStage(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers, Pipe ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(stmt);
        this.ring = ring;
    }

    public SQLServerStage(Connection conn, String sql, String message, FieldReferenceOffsetManager FROM, Pipe ring) throws SQLException {
        this.useMetaMessages = false;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
        this.message = message;
        this.FROM = FROM;
    }

    public SQLServerStage(PreparedStatement stmt, String message, FieldReferenceOffsetManager FROM, Pipe ring) throws SQLException {
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
            logger.error("SQLServerStage.run(): " + e.getClass().getName() + ": " + e.getMessage(), e);
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

                Object value = rs.getObject(col);

                switch (metadata.getColumnType(col)) {
                case Types.BIT: // -7
                    writeBooleanMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TINYINT: // -6
                    writeShortMessage(ring, isNullable, isSigned, columnName, value);
                    break;
                case Types.SMALLINT: // 5
                    writeShortMessage(ring, isNullable, isSigned, columnName, value);
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
                case Types.DATE: // 91
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIME: // 92
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIMESTAMP: // 93
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.DECIMAL: // 3
                    writeDecimalMessage(ring, isNullable, columnName, value);
                    break;
                case Types.NUMERIC: // 2
                    writeDecimalMessage(ring, isNullable, columnName, value);
                    break;
                case Types.NCHAR: // -15
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.NVARCHAR: // -9
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;  
                case Types.LONGNVARCHAR:  // -16
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.CHAR: // 1
                    writeASCIIMessage(ring, isNullable, columnName, value);
                    break;
                case Types.VARCHAR: // 12
                    writeASCIIMessage(ring, isNullable, columnName, value);
                    break;  
                case Types.LONGVARCHAR:  // -1
                    writeASCIIMessage(ring, isNullable, columnName, value);
                    break; 
                case Types.VARBINARY: // -3
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.BINARY: // -2
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case -155:  // datetimeoffset
                {
                    if (value == null) {
                        writeTimestampMessage(ring, isNullable, columnName, null, 0);
                    } else {
                        microsoft.sql.DateTimeOffset dto = (microsoft.sql.DateTimeOffset)value;
                        java.sql.Timestamp timestamp = dto.getTimestamp();
                        // http://msdn.microsoft.com/en-us/library/ff427226%28v=sql.110%29.aspx
                        int tzOffset = dto.getMinutesOffset();
                        writeTimestampMessage(ring, isNullable, columnName, timestamp, tzOffset);
                    }
                    break;       
                }
                    /*
                case Types.TIMESTAMP: // 93
                    // should really include nanos - resolution is limited to milliseconds otherwise
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.BINARY: // -2
                    if (value != null) {
                        UUID uuid = (UUID) value;
                        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
                        bb.putLong(uuid.getMostSignificantBits());
                        bb.putLong(uuid.getLeastSignificantBits());
                        writeByteArrayMessage(ring, isNullable, columnName, bb.array());
                    } else
                        writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.VARBINARY: // -3
                    writeByteArrayMessage(ring, isNullable, columnName, value);
                    break;
                case Types.OTHER: // 1111
                    writeSerializedMessage(ring, isNullable, columnName, value);
                    break;
                case Types.ARRAY: // 2003
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
                case Types.VARCHAR:
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                */
                default:
                    throw new NotImplementedException(metadata.getColumnTypeName(col) + "/" + metadata.getColumnType(col) + " not implemented for column " + col + "/" + metadata.getColumnName(col));
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
