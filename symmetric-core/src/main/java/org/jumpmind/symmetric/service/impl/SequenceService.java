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
package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlTransactionListenerAdapter;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Sequence;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISequenceService;

public class SequenceService extends AbstractService implements ISequenceService {

    private Map<String, Sequence> sequenceDefinitionCache = new HashMap<String, Sequence>();
    
    private Map<String, CachedRange> sequenceCache = new HashMap<String, CachedRange>();

    public SequenceService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
        setSqlMap(new SequenceServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public void init() {
        Map<String, Sequence> sequences = getAll();
        if (sequences.get(Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID) == null) {
            initSequence(Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID, 1, 0);
        }
        
        if (sequences.get(Constants.SEQUENCE_OUTGOING_BATCH) == null) {
            long maxBatchId = sqlTemplate.queryForLong(getSql("maxOutgoingBatchSql"));
            initSequence(Constants.SEQUENCE_OUTGOING_BATCH, maxBatchId, 10);
        }
        
        if (sequences.get(Constants.SEQUENCE_TRIGGER_HIST) == null) {
            long maxTriggerHistId = sqlTemplate.queryForLong(getSql("maxTriggerHistSql"));
            initSequence(Constants.SEQUENCE_TRIGGER_HIST, maxTriggerHistId, 0);
        }
        
        if (sequences.get(Constants.SEQUENCE_EXTRACT_REQ) == null) {
            long maxRequestId = sqlTemplate.queryForLong(getSql("maxExtractRequestSql"));
            initSequence(Constants.SEQUENCE_EXTRACT_REQ, maxRequestId, 0);
        }
    }
    
    private void initSequence(String name, long initialValue, int cacheSize) {
        try {
            if (initialValue < 1) {
                initialValue = 1;
            }
            create(new Sequence(name, initialValue, 1, 1, 9999999999l,
                    "system", false, cacheSize));
        } catch (UniqueKeyException ex) {
            log.debug("Failed to create sequence {}.  Must be initialized already.",
                    name);
        }
    }

    public synchronized long nextVal(String name) {
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED) && getSequenceDefinition(name).getCacheSize() > 0) {
            return nextValFromCache(null, name);
        }
        return nextValFromDatabase(name);
    }

    public synchronized long nextVal(ISqlTransaction transaction, final String name) {
        if (transaction != null) {
            transaction.addSqlTransactionListener(new SqlTransactionListenerAdapter() {
                @Override
                public void transactionRolledBack() {
                    sequenceCache.remove(name);
                }
            });
        }
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED) && getSequenceDefinition(transaction, name).getCacheSize() > 0) {
            return nextValFromCache(transaction, name);
        }
        return nextValFromDatabase(transaction, name);
    }

    protected long nextValFromCache(ISqlTransaction transaction, String name) {
        CachedRange range = sequenceCache.get(name);
        if (range != null) {
            long currentValue = range.getCurrentValue();
            if (currentValue < range.getEndValue()) {
                range.setCurrentValue(++currentValue);
                return currentValue;
            } else {
                sequenceCache.remove(name);
            }
        }
        return nextValFromDatabase(transaction, name);
    }
    
    protected long nextValFromDatabase(final String name) {
        return new DoTransaction<Long>() {
            public Long execute(ISqlTransaction transaction) {
                return nextValFromDatabase(transaction, name);
            }            
        }.execute();
    }

    protected long nextValFromDatabase(ISqlTransaction transaction, String name) {
        if (transaction == null) {
            return nextValFromDatabase(name);
        } else {
            long sequenceTimeoutInMs = parameterService.getLong(
                    ParameterConstants.SEQUENCE_TIMEOUT_MS, 5000);
            long ts = System.currentTimeMillis();
            do {
                long nextVal = tryToGetNextVal(transaction, name);
                if (nextVal > 0) {
                    return nextVal;
                }
            } while (System.currentTimeMillis() - sequenceTimeoutInMs < ts);

            throw new IllegalStateException(String.format(
                    "Timed out after %d ms trying to get the next val for %s",
                    System.currentTimeMillis() - ts, name));
        }
    }

    protected long tryToGetNextVal(ISqlTransaction transaction, String name) {
        long currVal = currVal(transaction, name);
        Sequence sequence = getSequenceDefinition(transaction, name);
        long nextVal = currVal + sequence.getIncrementBy();
        if (nextVal > sequence.getMaxValue()) {
            if (sequence.isCycle()) {
                nextVal = sequence.getMinValue();
            } else {
                throw new IllegalStateException(String.format(
                        "The sequence named %s has reached it's max value.  "
                                + "No more numbers can be handled out.", name));
            }
        } else if (nextVal < sequence.getMinValue()) {
            if (sequence.isCycle()) {
                nextVal = sequence.getMaxValue();
            } else {
                throw new IllegalStateException(String.format(
                        "The sequence named %s has reached it's min value.  "
                                + "No more numbers can be handled out.", name));
            }
        }

        CachedRange range = null;
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED) && sequence.getCacheSize() > 0) {
            long endVal = nextVal + (sequence.getIncrementBy() * (sequence.getCacheSize() - 1));
            range = new CachedRange(nextVal, endVal);
            nextVal = endVal;
        }

        int updateCount = transaction.prepareAndExecute(getSql("updateCurrentValueSql"), nextVal,
                name, currVal);
        if (updateCount != 1) {
            nextVal = -1;
        } else if (range != null) {
            sequenceCache.put(name, range);
            nextVal = range.getCurrentValue();
        }
        return nextVal;
    }

    protected Sequence getSequenceDefinition(final String name) {
        Sequence sequence = sequenceDefinitionCache.get(name);
        if (sequence != null) {
            return sequence;
        }

        return new DoTransaction<Sequence>() {
            public Sequence execute(ISqlTransaction transaction) {
                return getSequenceDefinition(transaction, name);
            }            
        }.execute();
    }

    protected Sequence getSequenceDefinition(ISqlTransaction transaction, String name) {
        Sequence sequence = sequenceDefinitionCache.get(name);
        if (sequence == null) {
            sequence = get(transaction, name);
            if (sequence != null) {
                sequenceDefinitionCache.put(name, sequence);
            } else {
                throw new IllegalStateException(String.format(
                        "The sequence named %s is not configured in %s", name,
                        TableConstants.getTableName(getTablePrefix(), TableConstants.SYM_SEQUENCE)));
            }
        }
        return sequence;
    }

    public synchronized long currVal(ISqlTransaction transaction, String name) {
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
            CachedRange range = sequenceCache.get(name);
            if (range != null) {
                return range.getCurrentValue();
            }
        }
        return transaction.queryForLong(getSql("getCurrentValueSql"), name);
    }

    public synchronized long currVal(final String name) {
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
            CachedRange range = sequenceCache.get(name);
            if (range != null) {
                return range.getCurrentValue();
            }
        }

        return new DoTransaction<Long>() {
            public Long execute(ISqlTransaction transaction) {
                return currVal(transaction, name);    
            }            
        }.execute();
    }

    public void create(Sequence sequence) {
        sqlTemplate.update(getSql("insertSequenceSql"), sequence.getSequenceName(),
                sequence.getCurrentValue(), sequence.getIncrementBy(), sequence.getMinValue(),
                sequence.getMaxValue(), sequence.isCycle() ? 1 : 0, sequence.getCacheSize(), sequence.getLastUpdateBy());
    }

    protected Sequence get(ISqlTransaction transaction, String name) {
        List<Sequence> values = transaction.query(getSql("getSequenceSql"), new SequenceRowMapper(), new Object[] {name}, new int [] {Types.VARCHAR});
        if (values.size() > 0) {
            return values.get(0);
        } else {
            return null;
        }
    }

    protected Map<String, Sequence> getAll() {
        Map<String, Sequence> map = new HashMap<String, Sequence>();
        List<Sequence> sequences = sqlTemplate.query(getSql("getAllSequenceSql"), new SequenceRowMapper());
        for (Sequence sequence : sequences) {
            map.put(sequence.getSequenceName(), sequence);
        }
        return map;
    }

    static class CachedRange {
        long currentValue;
        long endValue;
        
        public CachedRange(long currentValue, long endValue) {
            this.currentValue = currentValue;
            this.endValue = endValue;
        }

        public long getCurrentValue() {
            return currentValue;
        }
        
        public void setCurrentValue(long currentValue) {
            this.currentValue = currentValue;
        }
        
        public long getEndValue() {
            return endValue;
        }        
    }
    
    abstract class DoTransaction<T> {
        public T execute() {
            ISqlTransaction transaction = null;
            try {
                transaction = sqlTemplate.startSqlTransaction();
                T result = execute(transaction);
                transaction.commit();
                return result;
            } catch (Error ex) {
                if (transaction != null) {
                    transaction.rollback();
                }
                throw ex;
            } catch (RuntimeException ex) {
                if (transaction != null) {
                    transaction.rollback();
                }
                throw ex;              
            } finally {
                close(transaction);
            }
        }
        
        abstract public T execute(ISqlTransaction transaction);
    }

    static class SequenceRowMapper implements ISqlRowMapper<Sequence> {
        public Sequence mapRow(Row rs) {
            Sequence sequence = new Sequence();
            sequence.setCreateTime(rs.getDateTime("create_time"));
            sequence.setCurrentValue(rs.getLong("current_value"));
            sequence.setIncrementBy(rs.getInt("increment_by"));
            sequence.setLastUpdateBy(rs.getString("last_update_by"));
            sequence.setLastUpdateTime(rs.getDateTime("last_update_time"));
            sequence.setMaxValue(rs.getLong("max_value"));
            sequence.setMinValue(rs.getLong("min_value"));
            sequence.setSequenceName(rs.getString("sequence_name"));
            sequence.setCycle(rs.getBoolean("cycle_flag"));
            sequence.setCacheSize(rs.getInt("cache_size"));
            return sequence;
        }
    }

}
