package com.ociweb.pronghorn.components.sql.H2Component;

import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeBeginGroupMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeBooleanMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeByteArrayMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeByteMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDateTimeMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDecimalMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeDoubleMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeEndGroupMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeFloatMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeIntMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeLongMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeSerializedMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeShortMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeTimestampMessage;
import static com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageWriter.writeUTF8Message;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;

import com.ociweb.pronghorn.components.sql.DBUtil.DBUtil;
import com.ociweb.pronghorn.components.sql.DBUtil.Stmt;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;

public class H2Stage implements Runnable {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(H2Stage.class);
    private RingBuffer ring = null;
    private boolean emitFieldNames = false;
    private boolean emitRowMarkers = false;
    private Stmt stmt = null;
    private boolean useMetaMessages = false;
    private String message = null;
    private FieldReferenceOffsetManager FROM = null;

    public H2Stage(Connection conn, String sql, boolean emitFieldNames, boolean emitRowMarkers, RingBuffer ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
    }
    
    public H2Stage(PreparedStatement stmt, boolean emitFieldNames, boolean emitRowMarkers, RingBuffer ring) throws SQLException {
        this.useMetaMessages = true;
        this.emitFieldNames = emitFieldNames;
        this.emitRowMarkers = emitRowMarkers;
        this.stmt = new Stmt(stmt);
        this.ring = ring;
    }

    public H2Stage(Connection conn, String sql, String message, FieldReferenceOffsetManager FROM, RingBuffer ring) throws SQLException {
        this.useMetaMessages = false;
        this.stmt = new Stmt(conn, sql);
        this.ring = ring;
        this.message = message;
        this.FROM = FROM;
    }
    
    public H2Stage(PreparedStatement stmt, String message, FieldReferenceOffsetManager FROM, RingBuffer ring) throws SQLException {
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
            logger.error("H2Stage.run(): " + e.getClass().getName() + ": " + e.getMessage(), e);
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
                case Types.TINYINT: // -6
                    writeByteMessage(ring, isNullable, isSigned, columnName, value);
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
                case Types.DECIMAL: // 3
                    writeDecimalMessage(ring, isNullable, columnName, value);
                    break;
                case Types.DATE: // 91
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIME: // 92
                    writeDateTimeMessage(ring, isNullable, columnName, value);
                    break;
                case Types.TIMESTAMP: // 93
                    writeTimestampMessage(ring, isNullable, columnName, (java.sql.Timestamp)value, 0);
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
                case Types.VARCHAR: // 12
                    writeUTF8Message(ring, isNullable, columnName, value);
                    break;
                case Types.CLOB: // 2005
                {
                    // should really stream, but for now
                    org.h2.jdbc.JdbcClob clob = (org.h2.jdbc.JdbcClob)value;
                    String s = (value == null) ? null : clob.getSubString(1l, (int)clob.length());
                    writeUTF8Message(ring, isNullable, columnName, s);
                    break;
                }
                default:
                    throw new NotImplementedException(metadata.getColumnTypeName(col) + "/" + metadata.getColumnType(col) + " not implemented for column " + col + "/" + metadata.getColumnName(col));
                } // switch on column type
            } // column loop

            if (emitRowMarkers)
                writeEndGroupMessage(ring);

        } // while rs.next()
    }

    public void runFROM() throws SQLException {
            Map<String, Integer> fieldMap = new HashMap<String, Integer>();
            final int message_loc = lookupTemplateLocator(message, FROM);
            int templateID = (int)FROM.fieldIdScript[message_loc];
            logger.info("templateID: " + templateID);
            
            /*
            for (int col = 1; col <= metadata.getColumnCount(); col++) {
                String columnName = metadata.getColumnName(col);
                fieldMap.put(columnName, lookupFieldLocator(columnName, message_loc, FROM));   
                
                // if nullability can't be determined, assume it is nullable
                boolean isNullable = metadata.isNullable(col) != ResultSetMetaData.columnNoNulls;
                if (isNullable)
                    fieldMap.put(columnName + "_IsNull", lookupFieldLocator(columnName + "_IsNull", message_loc, FROM));
            }
            
            int field_loc = 0;
            */
            
            ResultSet rs = stmt.getStatement().executeQuery();
            ResultSetMetaData metadata = stmt.getMetadata();
            
            while (rs.next()) {
                logger.info("NEXT ROW");
                
                waitForRing(ring, message_loc);
                
                for (int col = 1; col <= metadata.getColumnCount(); col++) {

                    String columnName = metadata.getColumnName(col);
                    // if nullability can't be determined, assume it is nullable
                    boolean isNullable = metadata.isNullable(col) != ResultSetMetaData.columnNoNulls;
                    
                    Object value = rs.getObject(col);
                    if (isNullable) {
                        // field_loc = fieldMap.get(columnName + "_IsNull");
                        logger.info("writing " + columnName + "_IsNull: " + ((value == null) ? 1 : 0));
                        writeInt(ring, (value == null) ? 1 : 0);
                    }
                    
                    // for now, all fields must be written in order - eventually write using IDs
                    if (true || value != null) {
                        switch (metadata.getColumnType(col)) {
                        /*
                        case Types.TINYINT: // -6
                            writeByteMessage(ring, isNullable, isSigned, columnName, value);
                            break;
                        case Types.SMALLINT: // 5
                            writeShortMessage(ring, isNullable, isSigned, columnName, value);
                            break;
                        */
                        case Types.INTEGER: // 4
                            if (value == null) value = -1;
                            logger.info("writing " + columnName + ": " + (int)value);
                            writeInt(ring, (int)value);
                            break;
                        /*
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
                        case Types.DATE: // 91
                            writeDateTimeMessage(ring, isNullable, columnName, value);
                            break;
                        case Types.TIME: // 92
                            writeDateTimeMessage(ring, isNullable, columnName, value);
                            break;
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
                        */
                        default:
                            throw new NotImplementedException(metadata.getColumnTypeName(col) + " not implemented for column " + col + "/" + metadata.getColumnName(col));
                        } // switch on column type
                    }
                } // column loop
                
                RingBuffer.publishWrites(ring);

            } // while rs.next()


    }

    private void waitForRing(RingBuffer ring2, int message_loc) {
        throw new UnsupportedOperationException();
    }

    private void writeInt(RingBuffer ring2, int i) {
        throw new UnsupportedOperationException("This method should provide the LOC if using the high level api.");
    }
}
