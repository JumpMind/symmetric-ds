package org.jumpmind.db.platform.cassandra;

import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.AbstractSqlTemplate;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlResultsListener;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlStatementSource;
import org.jumpmind.db.sql.ISqlTransaction;

public class CassandraSqlTemplate extends AbstractSqlTemplate {

	@Override
	public byte[] queryForBlob(String sql, Object... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] queryForBlob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String queryForClob(String sql, Object... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String queryForClob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T queryForObject(String sql, Class<T> clazz, Object... params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> queryForMap(String sql, Object... params) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper, Object[] params, int[] types) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(boolean autoCommit, boolean failOnError, int commitRate, ISqlResultsListener listener,
			String... sql) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(boolean autoCommit, boolean failOnError, boolean failOnDrops, boolean failOnSequenceCreate,
			int commitRate, ISqlResultsListener listener, ISqlStatementSource source) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(String sql, Object[] values, int[] types) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void testConnection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isUniqueKeyViolation(Throwable ex) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isForeignKeyViolation(Throwable ex) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ISqlTransaction startSqlTransaction() {
		return new CassandraSqlTransaction();
	}

	@Override
	public ISqlTransaction startSqlTransaction(boolean autoCommit) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getDatabaseMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getDatabaseProductName() {
		return "cassandra";
	}

	@Override
	public String getDatabaseProductVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDriverName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDriverVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getSqlKeywords() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsGetGeneratedKeys() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isStoresUpperCaseIdentifiers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isStoresLowerCaseIdentifiers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isStoresMixedCaseQuotedIdentifiers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long insertWithGeneratedKey(String sql, String column, String sequenceName, Object[] args, int[] types) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isDataTruncationViolation(Throwable ex) {
		// TODO Auto-generated method stub
		return false;
	}

	
}
