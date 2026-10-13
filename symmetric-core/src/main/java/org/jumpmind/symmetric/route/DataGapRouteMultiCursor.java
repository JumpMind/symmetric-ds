/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.route;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.log.LogThrottle;
import org.jumpmind.log.ThrottledLogger;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;

public class DataGapRouteMultiCursor extends AbstractDataGapRouteCursor {
    protected int maxGapsToQualify;
    protected int gapIndex;
    protected int dataCount;
    protected int queryCount;
    protected int queryMillis;
    protected final LongSupplier currentSystemTime;
    protected final ThrottledLogger progressLog;

    public DataGapRouteMultiCursor(ChannelRouterContext context, ISymmetricEngine engine) {
        this(context, engine, System::currentTimeMillis);
    }

    DataGapRouteMultiCursor(ChannelRouterContext context, ISymmetricEngine engine, LongSupplier currentSystemTime) {
        super(context, engine);
        this.currentSystemTime = currentSystemTime;
        progressLog = new ThrottledLogger(log,
                new LogThrottle(currentSystemTime.getAsLong(), Constants.LONG_OPERATION_THRESHOLD, Constants.LONG_OPERATION_DEBUG_THRESHOLD, true));
        isEachGapQueried = true;
        maxGapsToQualify = parameterService.getInt(ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);
        cursor = prepareCursor();
    }

    @Override
    public Data next() {
        Data data = cursor.next();
        while (data == null && dataGaps.size() - gapIndex > 0) {
            cursor.close();
            cursor = prepareCursor();
            data = cursor.next();
        }
        if (data != null) {
            dataCount++;
        }
        return data;
    }

    protected ISqlReadCursor<Data> prepareCursor() {
        int gapsRemaining = dataGaps.size() - gapIndex;
        int numberOfGaps = gapsRemaining > maxGapsToQualify ? maxGapsToQualify : gapsRemaining;
        int numberOfArgs = 1 + (2 * numberOfGaps);
        Object[] args = new Object[numberOfArgs];
        int[] types = new int[numberOfArgs];
        int dataIdSqlType = engine.getSymmetricDialect().getSqlTypeForIds();
        List<DataGap> gapsToQualify = new ArrayList<DataGap>();
        args[0] = context.getChannel().getChannelId();
        types[0] = Types.VARCHAR;
        log.trace("Querying for gaps {} through {} of total {}", gapIndex + 1, gapIndex + numberOfGaps, dataGaps.size());
        for (int i = 1; i + 1 < numberOfArgs && gapIndex < dataGaps.size(); i += 2, gapIndex++) {
            DataGap gap = dataGaps.get(gapIndex);
            args[i] = gap.getStartId();
            types[i] = dataIdSqlType;
            args[i + 1] = gap.getEndId();
            types[i + 1] = dataIdSqlType;
            gapsToQualify.add(gap);
        }
        String sql = qualifyForOrder(qualifyUsingDataGaps(gapsToQualify, numberOfGaps, getSql("selectDataUsingGapsSql")));
        long ts = currentSystemTime.getAsLong();
        ISqlReadCursor<Data> cursor = executeCursor(sql, args, types);
        long currentTime = currentSystemTime.getAsLong();
        queryMillis += (currentTime - ts);
        queryCount++;
        context.incrementStat(1, ChannelRouterContext.STAT_MULTI_QUERY_COUNT);
        logProgress(currentTime);
        return cursor;
    }

    protected void logProgress(long currentTime) {
        if (progressLog.isDebugDue(currentTime)) {
            progressLog.infoOrDebug(currentTime, "Routing channel {} queried {} of {} gaps using {} queries with {} ms spent waiting on query executions",
                    context.getChannel().getChannelId(), gapIndex, dataGaps.size(), queryCount, queryMillis);
        }
    }

    @Override
    public void close() {
        super.close();
        String message = "Queried for {} gaps using {} queries with {} ms spent waiting on query executions";
        if (log.isDebugEnabled()) {
            log.debug(message, dataGaps.size(), queryCount, queryMillis);
        } else if (queryCount > 1 && queryMillis > 60000) {
            log.warn(message, dataGaps.size(), queryCount, queryMillis);
        }
    }
}
