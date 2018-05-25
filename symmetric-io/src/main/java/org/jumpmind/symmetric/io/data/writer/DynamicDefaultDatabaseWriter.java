package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.io.data.DataContext;

public class DynamicDefaultDatabaseWriter extends DefaultDatabaseWriter {

	private IDatabasePlatform targetPlatform;
	
	private ISqlTransaction targetTransaction;
	
	private String tablePrefix;
	
	public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
			String prefix) {
		super(symmetricPlatform);
		this.tablePrefix = prefix.toLowerCase();
		this.targetPlatform = targetPlatform;
	}
	
	public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
			String prefix, DatabaseWriterSettings settings) {
        super(symmetricPlatform, null, settings);
        this.tablePrefix = prefix.toLowerCase();
		this.targetPlatform = targetPlatform;
    }

    public DynamicDefaultDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform,
			String prefix, 
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        super(symmetricPlatform, conflictResolver, settings);
        this.tablePrefix = prefix.toLowerCase();
		this.targetPlatform = targetPlatform;
    }
    
    protected boolean isSymmetricTable(String tableName) {
		return tableName.toUpperCase().startsWith(this.tablePrefix.toUpperCase());
	}
	
    public boolean isLoadOnly() {
		return !this.platform.equals(this.targetPlatform);
	}
	
	@Override
	public IDatabasePlatform getPlatform(Table table) {
		if (table == null) {
			table = this.targetTable;
		}
		return table == null || isSymmetricTable(table.getNameLowerCase()) ?
			this.platform : this.targetPlatform;
	}
    
	@Override
	public IDatabasePlatform getPlatform(String table) {
		if (table == null) {
			table = this.targetTable.getNameLowerCase();
		} else {
			table = table.toLowerCase();
		}
		return table == null || isSymmetricTable(table) ?
				this.platform : this.targetPlatform;
	}

	@Override
	public IDatabasePlatform getPlatform() {
		return this.targetTable == null || isSymmetricTable(this.targetTable.getNameLowerCase()) ?
				super.platform : this.targetPlatform;
	}
	
	@Override
	public ISqlTransaction getTransaction() {
		return this.targetTable == null || isSymmetricTable(this.targetTable.getNameLowerCase()) 
				|| this.targetTransaction == null ?
				super.transaction : this.targetTransaction;
	}
	
	@Override
	public ISqlTransaction getTransaction(Table table) {
		if (table == null) {
			table = this.targetTable;
		}
		if (this.targetTransaction == null) {
			return this.transaction;
		}
		return table == null || isSymmetricTable(table.getNameLowerCase()) ?
			this.transaction : this.targetTransaction;
	}
	
	@Override
	public ISqlTransaction getTransaction(String table) {
		if (table == null) {
			table = this.targetTable.getNameLowerCase();
		} else {
			table = table.toLowerCase();
		}
		if (this.targetTransaction == null) {
			return this.transaction;
		}
		return table == null || isSymmetricTable(table.toLowerCase()) ?
			this.transaction : this.targetTransaction;
	}
	
	public ISqlTransaction getTargetTransaction() {
		return this.targetTransaction == null ? this.transaction : this.targetTransaction;
	}

	@Override
	public void open(DataContext context) {
		super.open(context);
		if (isLoadOnly()) {
			this.targetTransaction = this.targetPlatform.getSqlTemplate()
					.startSqlTransaction(!this.targetPlatform.supportsTransactions());
		}
	}
	
	@Override
	public void close() {
		super.close();
		if (isLoadOnly() && targetTransaction != null) {
	        this.targetTransaction.close();
	    }
	}
	
	@Override
	protected void commit(boolean earlyCommit) {
		super.commit(earlyCommit);
        if (isLoadOnly() && this.targetTransaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                this.targetTransaction.commit();
                if (!earlyCommit) {
                   notifyFiltersBatchCommitted();
                } else {
                    notifyFiltersEarlyCommit();
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
            }
        }
	}
	
	@Override
	protected void rollback() {
		super.rollback();
        if (isLoadOnly() && targetTransaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.LOADMILLIS);
                this.targetTransaction.rollback();
                notifyFiltersBatchRolledback();
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.LOADMILLIS);
            }
        }
	}

	public String getTablePrefix() {
		return tablePrefix;
	}
	
	
}
