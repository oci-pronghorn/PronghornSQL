package com.ociweb.pronghorn.components.sql.DBUtil;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;

public class DBTestUtil {
    // two hearts glyph (>0xFFFF): http://www.isthisthingon.org/unicode/index.phtml?glyph=1F495
    public static String unicodeTwoHeartsGlyph = new String(Character.toChars(0x1F495));
    // euro (<=0xFFFF) http://www.isthisthingon.org/unicode/index.phtml?glyph=A000
    public static String unicodeEuroGlyph = new String(Character.toChars(0x20AC));
    
    public static FieldReferenceOffsetManager metaFROM = MetaMessageDefs.FROM;
    
    public static List<Object> runTest(Runnable sqlStage, PronghornStage dumper) {
        throw new UnsupportedOperationException("Must be rewriten to match.");
//        try {
//            ExecutorService service = Executors.newFixedThreadPool(2);
//            service.submit(dumper);
//            service.submit(sqlStage).get(); // submit for execution, and wait for termination
//            dumper.stop();
//
//            service.shutdown();
//            service.awaitTermination(1, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            // ignore
//        } catch (ExecutionException e) {
//            // ignore
//        }
//
//        return dumper.result();
//        return null;
    }
    
    
    public static void executeSQL(Connection conn, String sql) throws SQLException {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
    
    public static void deleteFile(String path) {
        File f = new File(path);
        if (f.exists())
            f.delete();
    }
    
    public static void deleteDirectory(String directory) {
        File d = new File(directory);
        if (d.exists())
            try {
                FileUtils.deleteDirectory(d);
            } catch (IOException e) {
                // ignore
            }
    }
    
    public static java.util.Date getDate(int year, int month, int date, int hourOfDay, int minute, int second, int millisecond) {
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, date, hourOfDay, minute, second);
        cal.set(Calendar.MILLISECOND, millisecond);
        return cal.getTime();
    }
    
    public static String parseElement(Element config, String name, String defaultValue) throws IOException {
        NodeList nodes = config.getElementsByTagName(name);
        if (nodes.getLength() > 1) {
            throw new IOException("More than one " + name + " section in the config file");
        }
        return (nodes.getLength() > 0) ? nodes.item(0).getTextContent() : defaultValue;   
    }
    
    public static Element parseConfig() throws ParserConfigurationException, SAXException, IOException {
        // add -DtestConfig=file.xml on the command line to Maven
        String testConfig = System.getProperty("testConfig");
        Element config = null;
        if ((testConfig != null) && !testConfig.isEmpty()) {
            File xmlFile = new File(testConfig);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            NodeList configs = doc.getElementsByTagName("config");
            if (configs.getLength() != 1) {
                throw new IOException("More than one config section in the config file");
            }
            config = (Element)configs.item(0);
        }       
        return config;
    }
}
