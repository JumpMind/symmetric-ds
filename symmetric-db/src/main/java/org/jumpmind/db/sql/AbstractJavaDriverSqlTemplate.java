package org.jumpmind.db.sql;

import java.util.Map;
import java.util.Set;

public abstract class AbstractJavaDriverSqlTemplate extends AbstractSqlTemplate {

	public abstract String getDatabaseProductName();
	
//	@Override
//	public byte[] queryForBlob(String sql, Object... args) {
//		return null;
//	}

	@Override
	public byte[] queryForBlob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... args) {
		return null;
	}

//	@Override
//	public String queryForClob(String sql, Object... args) {
//		return null;
//	}

	@Override
	public String queryForClob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... args) {
		return null;
	}

	@Override
	public <T> T queryForObject(String sql, Class<T> clazz, Object... params) {
		return null;
	}

	@Override
	public Map<String, Object> queryForMap(String sql, Object... params) {
		return null;
	}

	@Override
	public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper, Object[] params, int[] types) {
		return null;
	}
	

    @Override
    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper, boolean returnLobObjects) {
        return null;
    }

	@Override
	public int update(boolean autoCommit, boolean failOnError, int commitRate, ISqlResultsListener listener,
			String... sql) {
		return 0;
	}

	@Override
	public int update(boolean autoCommit, boolean failOnError, boolean failOnDrops, boolean failOnSequenceCreate,
			int commitRate, ISqlResultsListener listener, ISqlStatementSource source) {
		return 0;
	}

	@Override
	public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
		return 0;
	}

	@Override
	public int update(String sql, Object[] values, int[] types) {
		return 0;
	}

	@Override
	public void testConnection() {
	}

	@Override
	public boolean isUniqueKeyViolation(Throwable ex) {
		return false;
	}

	@Override
	public boolean isDataTruncationViolation(Throwable ex) {
		return false;
	}

	@Override
	public boolean isForeignKeyViolation(Throwable ex) {
		return false;
	}

	@Override
	public ISqlTransaction startSqlTransaction() {
		return null;
	}

	@Override
	public ISqlTransaction startSqlTransaction(boolean autoCommit) {
		return null;
	}

	@Override
	public int getDatabaseMajorVersion() {
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() {
		return 0;
	}

	@Override
	public String getDatabaseProductVersion() {
		return null;
	}

	@Override
	public String getDriverName() {
		return null;
	}

	@Override
	public String getDriverVersion() {
		return null;
	}

	@Override
	public Set<String> getSqlKeywords() {
		return null;
	}

	@Override
	public boolean supportsGetGeneratedKeys() {
		return false;
	}

	@Override
	public boolean isStoresUpperCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean isStoresLowerCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean isStoresMixedCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public long insertWithGeneratedKey(String sql, String column, String sequenceName, Object[] args, int[] types) {
		return 0;
	}

    @Override
    public boolean isForeignKeyChildExistsViolation(Throwable ex) {
        return false;
    }
}
