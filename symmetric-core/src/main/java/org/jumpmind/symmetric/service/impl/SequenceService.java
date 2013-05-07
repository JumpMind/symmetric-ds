package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
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

    public SequenceService(IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
        setSqlMap(new SequenceServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public void init() {
        long maxBatchId = sqlTemplate.queryForLong(getSql("maxOutgoingBatchSql"));
        if (maxBatchId < 1) {
            maxBatchId = 1;
        }
        try {
            create(new Sequence(Constants.SEQUENCE_OUTGOING_BATCH, maxBatchId, 1, 1, 9999999999l,
                    "system", false));
        } catch (UniqueKeyException ex) {
            log.debug("Failed to create sequence {}.  Must be initialized already.",
                    Constants.SEQUENCE_OUTGOING_BATCH);
        }
        
        try {
            create(new Sequence(Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID, 1, 1, 1, 9999999999l,
                    "system", false));
        } catch (UniqueKeyException ex) {
            log.debug("Failed to create sequence {}.  Must be initialized already.",
                    Constants.SEQUENCE_OUTGOING_BATCH_LOAD_ID);
        }

    }

    public long nextVal(String name) {
        ISqlTransaction sqlTransaction = null;
        try {
            sqlTransaction = sqlTemplate.startSqlTransaction();
            return nextVal(sqlTransaction, name);
        } finally {
            close(sqlTransaction);
        }
    }

    public long nextVal(ISqlTransaction transaction, String name) {
        if (transaction == null) {
            return nextVal(name);
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

        int updateCount = transaction.prepareAndExecute(getSql("updateCurrentValueSql"), nextVal,
                name, currVal);
        if (updateCount != 1) {
            nextVal = -1;
        }

        return nextVal;
    }

    public long currVal(ISqlTransaction transaction, String name) {
        return transaction.queryForLong(getSql("getCurrentValueSql"), name);
    }

    public long currVal(String name) {
        ISqlTransaction sqlTransaction = null;
        try {
            sqlTransaction = sqlTemplate.startSqlTransaction();
            return currVal(sqlTransaction, name);
        } finally {
            close(sqlTransaction);
        }
    }

    public void create(Sequence sequence) {
        sqlTemplate.update(getSql("insertSequenceSql"), sequence.getSequenceName(),
                sequence.getCurrentValue(), sequence.getIncrementBy(), sequence.getMinValue(),
                sequence.getMaxValue(), sequence.isCycle() ? 1 : 0, sequence.getLastUpdateBy());
    }

    protected Sequence get(ISqlTransaction transaction, String name) {
        List<Sequence> values = transaction.query(getSql("getSequenceSql"), new SequenceRowMapper(), new Object[] {name}, new int [] {Types.VARCHAR});
        if (values.size() > 0) {
            return values.get(0);
        } else {
            return null;
        }
    }

    class SequenceRowMapper implements ISqlRowMapper<Sequence> {
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
            sequence.setCycle(rs.getBoolean("cycle"));
            return sequence;
        }
    }

}
