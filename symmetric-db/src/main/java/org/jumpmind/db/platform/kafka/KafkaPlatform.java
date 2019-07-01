package org.jumpmind.db.platform.kafka;

import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class KafkaPlatform extends AbstractDatabasePlatform {

	public KafkaPlatform(SqlTemplateSettings settings) {
		super(settings);
		super.ddlBuilder = new KafkaDdlBuilder();
		super.ddlReader = new KafkaDdlReader(this);
		supportsTruncate = false;
	}
	
	@Override
	public String getName() {
		return "kafka";
	}
	
	@Override
	public String getDefaultSchema() {
		return null;
	}

	@Override
	public String getDefaultCatalog() {
		return null;
	}

	@Override
	public <T> T getDataSource() {
		return null;
	}

	@Override
	public boolean isLob(int type) {
		return false;
	}

	@Override
	public ISqlTemplate getSqlTemplate() {
		return new KafkaSqlTemplate();
	}

	@Override
	public ISqlTemplate getSqlTemplateDirty() {
		return new KafkaSqlTemplate();
	}
}
