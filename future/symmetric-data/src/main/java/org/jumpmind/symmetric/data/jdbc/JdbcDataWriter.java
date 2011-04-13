package org.jumpmind.symmetric.data.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.BinaryEncoding;
import org.jumpmind.symmetric.data.IDataWriter;
import org.jumpmind.symmetric.model.Data;

public class JdbcDataWriter implements IDataWriter<JdbcDataContext> {

    protected DataSource dataSource;

    @Override
    public JdbcDataContext createDataContext() {
        return new JdbcDataContext();
    }

    public JdbcDataWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void open(String nodeId, BinaryEncoding encoding, JdbcDataContext context) {
        try {
            context.setConnection(dataSource.getConnection());
            context.setOldAutoCommitValue(context.getConnection().getAutoCommit());
            context.getConnection().setAutoCommit(false);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void switchTables(JdbcDataContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void startBatch(JdbcDataContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeData(Data data, JdbcDataContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close(JdbcDataContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void finishBatch(JdbcDataContext context) {
        try {
            context.getConnection().commit();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

}
