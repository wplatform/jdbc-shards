package com.wplatform.ddal.excutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.wplatform.ddal.util.JdbcUtils;

public class UpdateResult {

    private final Connection conn;
    private final Statement stmt;
    private final int affectRows;

    public UpdateResult(Connection conn, Statement stmt, int affectRows) {
        super();
        this.conn = conn;
        this.stmt = stmt;
        this.affectRows = affectRows;
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return stmt.getGeneratedKeys();
    }

    public int getAffectRows() {
        return affectRows;
    }

    public void close() {
        JdbcUtils.closeSilently(stmt);
        JdbcUtils.closeSilently(conn);
    }


}
