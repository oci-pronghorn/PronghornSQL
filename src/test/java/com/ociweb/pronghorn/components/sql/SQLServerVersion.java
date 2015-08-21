package com.ociweb.pronghorn.components.sql;

public class SQLServerVersion {
    private int majorVersion;
    private int minorVersion;
    private int build;
    private String productLevel;
    private String edition;
    
    public SQLServerVersion(int majorVersion, int minorVersion, int build, String productLevel, String edition) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.build = build;
        this.productLevel = productLevel;
        this.edition = edition;
    }
    
    public int getMajorVersion() {
        return majorVersion;
    }
    
    public int getMinorVersion() {
        return minorVersion;
    }
    
    public int getBuild() {
        return build;
    }
    
    public String getProductLevel() {
        return productLevel;
    }
    
    public String getEdition() { 
        return edition;
    }
}
