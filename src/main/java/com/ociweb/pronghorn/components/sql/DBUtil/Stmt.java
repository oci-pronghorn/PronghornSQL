package com.ociweb.pronghorn.components.sql.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Stmt {
    private PreparedStatement stmt = null;
    private boolean ownStatement = false;
    private ResultSetMetaData metadata = null;
    
    public Stmt(Connection conn, String sql) throws SQLException {
        this.stmt = conn.prepareStatement(sql);
        ownStatement = true;
        this.metadata = stmt.getMetaData();
    }
    
    public Stmt(PreparedStatement stmt) throws SQLException {
        this.stmt = stmt;
        ownStatement = false;
        this.metadata = stmt.getMetaData();
    }
    
    public void close() throws SQLException {
        if (ownStatement && (stmt != null)) {
            stmt.close();
        }
    }
    
    public PreparedStatement getStatement() {
        return stmt;
    }
    
    public ResultSetMetaData getMetadata() {
        return metadata;
    }
}
