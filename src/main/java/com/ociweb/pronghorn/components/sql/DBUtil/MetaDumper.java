package com.ociweb.pronghorn.components.sql.DBUtil;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.util.List;

public class MetaDumper extends UserDumper {
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetaDumper.class);

    public MetaDumper(GraphManager gm, Pipe ring) {
        super(gm, ring, new MetaDecoder());
    }
}


class MetaDecoder implements UserDumper.Decoder {
        public boolean decode(Pipe ring, int templateID, List<Object> output) throws Exception {
            switch (templateID) {
            case 128: // UInt32
            {
                int value = PipeReader.readInt(ring, MetaMessageDefs.UINT32_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 130: // Int32
            {
                int value = PipeReader.readInt(ring, MetaMessageDefs.INT32_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 134: // Int64
            {
                long value = PipeReader.readLong(ring, MetaMessageDefs.INT64_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 136: // ASCII 
            { 
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.ASCII_VALUE_LOC, sb);
                output.add(sb.toString()); 
                return true; 
            }
             case 138: // UTF8 
             { 
                 StringBuffer sb = new StringBuffer();
                 PipeReader.readUTF8(ring, MetaMessageDefs.UTF8_VALUE_LOC, sb);
                 output.add(sb.toString()); 
                 return true; 
             }
            case 140: // Decimal
            {
                int exponent = PipeReader.readDecimalExponent(ring, MetaMessageDefs.DECIMAL_VALUE_LOC);
                long mantissa = PipeReader.readDecimalMantissa(ring, MetaMessageDefs.DECIMAL_VALUE_LOC);
                output.add(BigDecimal.valueOf(mantissa, exponent));
                return true;
            }
            case 142: // ByteArray
            {
                // byte[] bytes = new byte[ring.maxAvgVarLen];
                int len = PipeReader.readDataLength(ring, MetaMessageDefs.BYTEARRAY_VALUE_LOC);
                byte[] value = new byte[len];
                PipeReader.readBytes(ring, MetaMessageDefs.BYTEARRAY_VALUE_LOC, value, 0);
                output.add(value);
                return true;
            }
            case 144: // BeginGroup
            {
                output.add("**BEGIN GROUP**");
                return true;
            }
            case 146: // EndGroup
            {
                output.add("**END GROUP**");
                return true;
            }
            case 166: // Boolean
            {
                int value = PipeReader.readInt(ring, MetaMessageDefs.BOOLEAN_VALUE_LOC);
                output.add(value == 1);
                return true;
            }
            case 168: // Float
            {
                int bits = PipeReader.readInt(ring, MetaMessageDefs.FLOAT_VALUE_LOC);
                float value = Float.intBitsToFloat(bits);
                output.add(value);
                return true;
            }
            case 170: // Double
            {
                long bits = PipeReader.readLong(ring, MetaMessageDefs.DOUBLE_VALUE_LOC);
                double value = Double.longBitsToDouble(bits);
                output.add(value);
                return true;
            }
            case 172: // DateTime
            {
                long millisecondsSinceEpoch = PipeReader.readLong(ring, MetaMessageDefs.DATETIME_VALUE_LOC);
                java.util.Date value = new java.util.Date(millisecondsSinceEpoch);
                output.add(value);
                return true;
            }
            case 174: // SerializedJavaObject
            {
                Object value = null;
                int len = PipeReader.readDataLength(ring, MetaMessageDefs.SERIALIZEDJAVAOBJECT_VALUE_LOC);
                byte[] bytes = new byte[len];
                PipeReader.readBytes(ring, MetaMessageDefs.SERIALIZEDJAVAOBJECT_VALUE_LOC, bytes, 0);
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in = null;
                try {
                    in = new ObjectInputStream(bis);
                    value = in.readObject();
                } finally {
                    try {
                        if (in != null)
                            in.close();
                    } catch (IOException ex) {
                        // ignore close exception
                    }
                    try {
                        bis.close();
                    } catch (IOException ex) {
                        // ignore close exception
                    }

                }

                output.add(value);
                return true;
            }
            case 176: // Timestamp
            {
                long millisecondsSinceEpoch = PipeReader.readLong(ring, MetaMessageDefs.TIMESTAMP_DATETIME_LOC);
                java.util.Date value = new java.util.Date(millisecondsSinceEpoch);
                output.add(value);
                output.add(PipeReader.readInt(ring, MetaMessageDefs.TIMESTAMP_NANOS_LOC));
                output.add(PipeReader.readInt(ring, MetaMessageDefs.TIMESTAMP_TZOFFSET_LOC));
                return true;
            }
            case 192: // NamedUInt32
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDUINT32_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                int value = PipeReader.readInt(ring, MetaMessageDefs.NAMEDUINT32_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 194: // NamedInt32
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDINT32_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                int value = PipeReader.readInt(ring, MetaMessageDefs.NAMEDINT32_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 198: // NamedInt64
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDINT64_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                long value = PipeReader.readLong(ring, MetaMessageDefs.NAMEDINT64_VALUE_LOC);
                output.add(value);
                return true;
            }
            case 200: // NamedASCII
            { 
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDASCII_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDASCII_VALUE_LOC, sb);
                output.add(sb.toString()); 
                return true; 
            }
            case 202: // NamedUTF8 
            { 
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDUTF8_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                sb = new StringBuffer();
                PipeReader.readUTF8(ring, MetaMessageDefs.NAMEDUTF8_VALUE_LOC, sb);
                output.add(sb.toString()); 
                return true; 
            }
            case 204: // NamedDecimal
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDDECIMAL_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                int exponent = PipeReader.readDecimalExponent(ring, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);
                long mantissa = PipeReader.readDecimalMantissa(ring, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);
                output.add(BigDecimal.valueOf(mantissa, exponent));
                return true;
            }
            case 206: // NamedByteArray
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDDECIMAL_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                // byte[] bytes = new byte[ring.maxAvgVarLen];
                int len = PipeReader.readDataLength(ring, MetaMessageDefs.NAMEDBYTEARRAY_VALUE_LOC);
                byte[] value = new byte[len];
                PipeReader.readBytes(ring, MetaMessageDefs.NAMEDBYTEARRAY_VALUE_LOC, value, 0);
                output.add(value);
                return true;
            }
            case 230: // NamedBoolean
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDBOOLEAN_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                int value = PipeReader.readInt(ring, MetaMessageDefs.NAMEDBOOLEAN_VALUE_LOC);
                output.add(value == 1);
                return true;
            }
            case 232: // NamedFloat
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDFLOAT_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                int bits = PipeReader.readInt(ring, MetaMessageDefs.NAMEDFLOAT_VALUE_LOC);
                float value = Float.intBitsToFloat(bits);
                output.add(value);
                return true;
            }
            case 234: // NamedDouble
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDDOUBLE_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                long bits = PipeReader.readLong(ring, MetaMessageDefs.NAMEDDOUBLE_VALUE_LOC);
                double value = Double.longBitsToDouble(bits);
                output.add(value);
                return true;
            }
            case 236: // NamedDateTime
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDDATETIME_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                long millisecondsSinceEpoch = PipeReader.readLong(ring, MetaMessageDefs.NAMEDDATETIME_VALUE_LOC);
                java.util.Date value = new java.util.Date(millisecondsSinceEpoch);
                output.add(value);
                return true;
            }
            case 238: // NamedSerializedJavaObject
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDSERIALIZEDJAVAOBJECT_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                Object value = null;
                int len = PipeReader.readDataLength(ring, MetaMessageDefs.NAMEDSERIALIZEDJAVAOBJECT_VALUE_LOC);
                byte[] bytes = new byte[len];
                PipeReader.readBytes(ring, MetaMessageDefs.NAMEDSERIALIZEDJAVAOBJECT_VALUE_LOC, bytes, 0);
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInput in = null;
                try {
                    in = new ObjectInputStream(bis);
                    value = in.readObject();
                } finally {
                    try {
                        if (in != null)
                            in.close();
                    } catch (IOException ex) {
                        // ignore close exception
                    }
                    try {
                        bis.close();
                    } catch (IOException ex) {
                        // ignore close exception
                    }

                }

                output.add(value);
                return true;
            }
            case 240: // NamedTimestamp
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDTIMESTAMP_NAME_LOC, sb);
                String s = sb.toString();
                output.add(s);
                long millisecondsSinceEpoch = PipeReader.readLong(ring, MetaMessageDefs.NAMEDTIMESTAMP_DATETIME_LOC);
                java.util.Date value = new java.util.Date(millisecondsSinceEpoch);
                output.add(value);
                output.add(PipeReader.readInt(ring, MetaMessageDefs.NAMEDTIMESTAMP_NANOS_LOC));
                output.add(PipeReader.readInt(ring, MetaMessageDefs.NAMEDTIMESTAMP_TZOFFSET_LOC));
                return true;
            }
            case 384: // NullableUInt32
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEUINT32_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 386: // NullableInt32
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEINT32_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 390: // NullableInt64
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEINT64_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 392: // NullableASCII
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEASCII_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 394: // NullableUTF8
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEUTF8_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 396: // NullableDecimal
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEDECIMAL_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 398: // NullableByteArray
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEBYTEARRAY_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 423: // NullableBoolean
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEBOOLEAN_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 425: // NullableFloat
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEFLOAT_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 427: // NullableDouble
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEDOUBLE_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 429: // NullableDateTime
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLEDATETIME_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 431: // NullableSerializedJavaObject
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLESERIALIZEDJAVAOBJECT_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }
            case 433: // NullableTimestamp
            {
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NULLABLETIMESTAMP_NOTNULL_LOC);
                if (notNull == 0)
                    output.add(null);
                return true;
            }            
            case 448: // NamedNullableUInt32
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEUINT32_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEUINT32_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 450: // NamedNullableInt32
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEINT32_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEINT32_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 454: // NamedNullableInt64
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEINT64_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEINT64_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 456: // NamedNullableASCII
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEASCII_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEASCII_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 458: // NamedNullableUTF8
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEUTF8_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEUTF8_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 460: // NamedNullableDecimal
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEDECIMAL_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEDECIMAL_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 462: // NamedNullableByteArray
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEBYTEARRAY_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEBYTEARRAY_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 487: // NamedNullableBoolean
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEBOOLEAN_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEBOOLEAN_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 489: // NamedNullableFloat
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEFLOAT_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEFLOAT_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 491: // NamedNullableDouble
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEDOUBLE_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEDOUBLE_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 493: // NamedNullableDateTime
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLEDATETIME_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLEDATETIME_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 495: // NamedNullableSerializedJavaObject
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLESERIALIZEDJAVAOBJECT_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLESERIALIZEDJAVAOBJECT_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            case 497: // NamedNullableTimestamp
            {
                StringBuffer sb = new StringBuffer();
                PipeReader.readASCII(ring, MetaMessageDefs.NAMEDNULLABLETIMESTAMP_NAME_LOC, sb);
                String s = sb.toString();
                int notNull = PipeReader.readInt(ring, MetaMessageDefs.NAMEDNULLABLETIMESTAMP_NOTNULL_LOC);
                if (notNull == 0) {
                    output.add(s);
                    output.add(null);
                }
                return true;
            }
            default:
                return false;
            } // switch templateID
        }
    }
