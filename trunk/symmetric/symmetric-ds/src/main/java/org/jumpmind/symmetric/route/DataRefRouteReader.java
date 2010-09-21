package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for other threads to
 * process.
 */
public class DataRefRouteReader extends AbstractDataToRouteReader {

    public static final String SELECT_DATA_TO_BATCH_SQL = "selectDataToBatchSql";

    public DataRefRouteReader(DataSource dataSource, int queryTimeout, int maxQueueSize,
            ISqlProvider sqlProvider, int fetchSize, RouterContext context, IDataService dataService, boolean requiresAutoCommitFalse) {
        super(dataSource, queryTimeout, maxQueueSize, sqlProvider, fetchSize, context, dataService, requiresAutoCommitFalse);
    }

    @Override
    protected PreparedStatement prepareStatment(Connection c) throws SQLException {
        DataRef dataRef = dataService.getDataRef();
        String channelId = context.getChannel().getChannelId();
        PreparedStatement ps = c.prepareStatement(getSql(SELECT_DATA_TO_BATCH_SQL, context.getChannel().getChannel()),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(queryTimeout);
        ps.setFetchSize(fetchSize);
        ps.setString(1, channelId);
        ps.setLong(2, dataRef.getRefDataId());
        return ps;
    }
    
    @Override
    protected PreparedStatement prepareSecondaryStatement(Connection c) throws SQLException {     
        return prepareStatment(c);
    }

}
