package com.ociweb.pronghorn.components.sql.DBUtil;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;


public class DBUtil  {

    public static FieldReferenceOffsetManager buildFROM(String source) {
        try {
            return TemplateHandler.loadFrom(source);
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("buildFrom "+source, e);
        } catch (SAXException e) {
            throw new RuntimeException("buildFrom "+source, e);
        } catch (IOException e) {
            throw new RuntimeException("buildFrom "+source, e);
        }
    }

}
