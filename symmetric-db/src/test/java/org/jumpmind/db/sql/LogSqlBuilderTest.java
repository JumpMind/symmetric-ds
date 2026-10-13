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
package org.jumpmind.db.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import org.jumpmind.util.FormatUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.MDC;

public class LogSqlBuilderTest {
    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = mock(Logger.class);
        when(logger.isDebugEnabled()).thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        MDC.remove("engineName");
    }

    @Test
    void testLogSql_fiveArgOverload_delegatesWithNullMessage() {
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, "select 1", null, null, 5L);
        verify(logger).debug("(5ms.) select 1");
        verify(logger, never()).info(anyString());
    }

    @Test
    void testLogSql_sixArgOverload_withMessage_prependsMessageToLogEntry() {
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, "context", "select 1", null, null, 5L);
        verify(logger).debug("(5ms.) context select 1");
    }

    @Test
    void testLogSql_whenExecutionTimeExceedsDefaultThreshold_logsAtInfoWithLongRunningPrefix() {
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, null, "select 1", null, null, 20000L);
        verify(logger).info("Long Running: (20.0 seconds) select 1");
        verify(logger, never()).debug(anyString());
    }

    @Test
    void testLogSql_whenLongRunning_formatsSecondsWithOneDigitAfterPeriod() {
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, null, "select 1", null, null, 23456L);
        verify(logger).info("Long Running: (23.5 seconds) select 1");
    }

    @Test
    void testLogSql_withGuiEngine_usesLowerConsoleThresholdForLongRunning() {
        MDC.put("engineName", "gui");
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, null, "select 1", null, null, 5000L);
        verify(logger).info("Long Running: (5.0 seconds) select 1");
    }

    @Test
    void testLogSql_withParametersInlineFalse_logsRawSqlThenSeparateArgsLine() {
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.setLogSqlParametersInline(false);
        builder.logSql(logger, null, "select * from t where id = ?", new Object[] { 1 }, null, 5L);
        verify(logger).debug("(5ms.) select * from t where id = ?");
        verify(logger).debug("sql args: {}", "[1]");
    }

    @Test
    void testLogSql_whenLoggerNotDebugEnabledAndNotLongRunning_logsNothing() {
        when(logger.isDebugEnabled()).thenReturn(false);
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, null, "select 1", null, null, 5L);
        verify(logger, never()).debug(anyString());
        verify(logger, never()).info(anyString());
    }

    @Test
    void testLogSql_whenLoggerTraceEnabled_appendsTrailingCarriageReturn() {
        when(logger.isTraceEnabled()).thenReturn(true);
        LogSqlBuilder builder = new LogSqlBuilder();
        builder.logSql(logger, null, "select 1", null, null, 5L);
        verify(logger).debug("(5ms.) select 1\r\n");
    }

    @Test
    void testLogSqlAfterException_returnsSameExceptionAndLogsMessageWithArgs() {
        LogSqlBuilder builder = new LogSqlBuilder();
        SQLException ex = new SQLException("boom");
        SQLException result = builder.logSqlAfterException(logger, "select * from t where id = ?", new Object[] { 1 }, ex);
        assertSame(ex, result);
        verify(logger).debug("SQL caused exception: [select * from t where id = ?] sql args: [1] " + ex);
    }

    @Test
    void testLogSqlAfterException_withNoArgs_omitsArgsSegment() {
        LogSqlBuilder builder = new LogSqlBuilder();
        SQLException ex = new SQLException("boom");
        builder.logSqlAfterException(logger, "select 1", null, ex);
        verify(logger).debug("SQL caused exception: [select 1] " + ex);
    }

    @Test
    public void testNoPlachodlers() {
        final String SQL = "select * from sym_data where data_id = 234234;";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, null, null);
        assertEquals(SQL, result);
    }

    @Test
    public void testSinglePlachodler() {
        final String SQL = "select * from sym_data where data_id = ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { Integer.valueOf(234234) }, new int[] { Types.BIGINT });
        System.out.println(result);
        assertEquals("select * from sym_data where data_id = 234234", result);
    }

    @Test
    public void testSinglePlachodlerNoTypes() {
        final String SQL = "select * from sym_data where data_id = ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { Integer.valueOf(234234) }, null);
        System.out.println(result);
        assertEquals("select * from sym_data where data_id = 234234", result);
    }

    @Test
    public void testSinglePlachodlerNoTypesString() {
        final String SQL = "select * from sym_data where data_id = ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { "234234" }, null);
        System.out.println(result);
        assertEquals("select * from sym_data where data_id = '234234'", result);
    }

    @Test
    public void testMultiPlaceholders() {
        final String SQL = "select * from sym_data where data_id between ? and ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { 12, 21 }, new int[] { Types.BIGINT, Types.BIGINT });
        System.out.println(result);
        assertEquals("select * from sym_data where data_id between 12 and 21", result);
    }

    @Test
    public void testTypes() {
        final String SQL = "update sym_data set data = ? where data_id =? and create_time > ? and create_time <= ? and table_name = ? and time>=? and blob_colum = ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        Date date = FormatUtils.parseDate("2016-04-20 17:12:57", FormatUtils.TIMESTAMP_PATTERNS);
        Timestamp ts = new Timestamp(date.getTime());
        Time time = new Time(date.getTime());
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { "\"002\",\"hostname\"", 21, date, ts, "sym_node_host", time, new byte[] { 0, 2, 8 } },
                new int[] { Types.CLOB, Types.BIGINT, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.TIME, Types.BINARY });
        System.out.println(result);
        assertEquals(
                "update sym_data set data = '\"002\",\"hostname\"' where data_id =21 and create_time > {ts '2016-04-20 17:12:57.000'} and create_time <= {ts '2016-04-20 17:12:57.000000000'} and table_name = 'sym_node_host' and time>={t '17:12:57.000000000'} and blob_colum = '000208'",
                result);
    }

    @Test
    public void testEscapes() {
        final String SQL = "update sym_data set table_name = ?";
        LogSqlBuilder builder = new LogSqlBuilder();
        String result = builder.buildDynamicSqlForLog(SQL, new Object[] { "'?\\/" }, new int[] { Types.CLOB });
        System.out.println(result);
        assertEquals("update sym_data set table_name = '''?\\/'", result);
    }
}
