/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.extract;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import javax.sql.DataSource;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataExtractorTest extends AbstractTest
{
    private static final String SERVICE_NAME = "dataExtractor";
    private static final String RUNTIME_CONFIG = "runtimeConfiguration";
    private static final String CONTEXT_NAME = "extractorContext";
    private static final String TABLE_NAME = "table1";
    
    private final TestData TD1 = new TestData(999, "foo", "\"abc\", 123, \"xyz\"", "328", 
        "basket_id", "mango, watermellon, grape");
    private final TestData TD2 = new TestData(998, "foo", "\"www\", 888, \"ghi\"", "6578", 
        "basket_id", "mango, watermellon, grape");
    private final TestData TD3 = new TestData(997, "foo", "\"monday\", 879, \"ggg\"", "6502", 
        "basket_id", "grape, tomato, cucumber");
    private final TestData TD4 = new TestData(997, "bar", "\"monday\", 879, \"ggg\"", "6502", 
        "basket_id", "grape, tomato, cucumber");
    
    @Test(groups="continuous")
    public void basicTest() 
    {   
        TriggerHistory audit = makeTableSyncAuditId(TD1.keyColumns, TD1.columns);
        IDataExtractor dataExtractor = (IDataExtractor) getBeanFactory().getBean(SERVICE_NAME);
        
        try
        {
            IRuntimeConfig runtimeConfiguration = (IRuntimeConfig) getBeanFactory().getBean(RUNTIME_CONFIG);
            DataExtractorContext context = (DataExtractorContext) getBeanFactory().getBean(CONTEXT_NAME);
            
            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            String batchId = "cafebabe";
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);
            
            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, 
                TD1.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            dataExtractor.commit(batch, writer);
            
            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(runtimeConfiguration.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.batchEnd(batchId);
      
            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        }
        catch (IOException e)
        {
            Assert.fail("BasicTeset failed", e);
        }
        
        cleanup();
    }
    @Test(groups="continuous")
    public void biggerTest() 
    {   
        TriggerHistory audit = makeTableSyncAuditId(TD1.keyColumns, TD1.columns);
        IDataExtractor dataExtractor = (IDataExtractor) getBeanFactory().getBean(SERVICE_NAME);
        
        try
        {
            IRuntimeConfig runtimeConfiguration = (IRuntimeConfig) getBeanFactory().getBean(RUNTIME_CONFIG);
            DataExtractorContext context = (DataExtractorContext) getBeanFactory().getBean(CONTEXT_NAME);
            
            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            String batchId = "cafebabe";
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);
            
            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, 
                TD1.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            
            data =  new Data(TD2.dataId, TD2.key, TD2.rowData, DataEventType.UPDATE, 
                TD2.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            dataExtractor.commit(batch, writer);
            
            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(runtimeConfiguration.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.update(TD2.rowData, TD2.key);
            em.batchEnd(batchId);
        
            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        }
        catch (IOException e)
        {
            Assert.fail("BasicTeset failed", e);
        }
        
        cleanup();
    }  
    
    @Test(groups="continuous")
    public void notherTest() 
    {   
        IDataExtractor dataExtractor = (IDataExtractor) getBeanFactory().getBean(SERVICE_NAME);
        
        try
        {
            IRuntimeConfig runtimeConfiguration = (IRuntimeConfig) getBeanFactory().getBean(RUNTIME_CONFIG);
            DataExtractorContext context = (DataExtractorContext) getBeanFactory().getBean(CONTEXT_NAME);
            
            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            String batchId = "cafebabe";
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);

            TriggerHistory audit = makeTableSyncAuditId(TD1.keyColumns, TD1.columns);
            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, 
                TD1.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            
            audit = makeTableSyncAuditId(TD3.keyColumns, TD3.columns);
            data =  new Data(TD3.dataId, TD3.key, TD3.rowData, DataEventType.UPDATE, 
                TD3.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            data =  new Data(TD3.dataId, TD3.key, TD3.rowData, DataEventType.DELETE, 
                TD3.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            dataExtractor.commit(batch, writer);
            
            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(runtimeConfiguration.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.table(TD3.table, TD3.keyColumns, TD3.columns);
            em.update(TD3.rowData, TD3.key);
            em.delete(TD3.key);
            em.batchEnd(batchId);
       
            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        }
        catch (IOException e)
        {
            Assert.fail("BasicTest failed", e);
        }
        
        cleanup();
    } 
    
    @Test(groups="continuous")
    public void changingTables() 
    {   
        TriggerHistory audit = makeTableSyncAuditId(TD1.keyColumns, TD1.columns);
        TriggerHistory audit2 = makeTableSyncAuditId(TD4.keyColumns, TD4.columns);
        IDataExtractor dataExtractor = (IDataExtractor) getBeanFactory().getBean(SERVICE_NAME);
        
        try
        {
            IRuntimeConfig runtimeConfiguration = (IRuntimeConfig) getBeanFactory().getBean(RUNTIME_CONFIG);
            DataExtractorContext context = (DataExtractorContext) getBeanFactory().getBean(CONTEXT_NAME);
            
            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            String batchId = "cafebabe";
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);
            
            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, 
                TD1.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            
            data = new Data(TD4.dataId, TD4.key, TD4.rowData, DataEventType.UPDATE, 
                TD4.table, new Date(), audit2);
            dataExtractor.write(writer, data, context);
            
            data =  new Data(TD2.dataId, TD2.key, TD2.rowData, DataEventType.UPDATE, 
                TD2.table, new Date(), audit);
            dataExtractor.write(writer, data, context);
            dataExtractor.commit(batch, writer);
            
            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(runtimeConfiguration.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.table(TD4.table, TD4.keyColumns, TD4.columns);
            em.update(TD4.rowData, TD4.key);
            em.table(TD1.table);
            em.update(TD2.rowData, TD2.key);
            em.batchEnd(batchId);
        
            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        }
        catch (IOException e)
        {
            Assert.fail("BasicTeset failed", e);
        }
        
        cleanup();
    }  
    
    protected void cleanup() 
    {
        this.getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException
            {
                Statement s = connection.createStatement();
                s.executeUpdate("delete from sym_trigger_hist where source_table_name = '" +TABLE_NAME+ "'");
                return null;
            }
        });
    }
    
    
    private TriggerHistory makeTableSyncAuditId(final String pk, final String col) 
    {
        return (TriggerHistory) this.getJdbcTemplate().execute(new ConnectionCallback()
        {
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException
            {
                Statement s = connection.createStatement();
                s.executeUpdate("insert into sym_trigger_hist(source_table_name, source_schema_name, trigger_id, column_names, pk_column_names,name_for_update_trigger,name_for_delete_trigger, name_for_insert_trigger,table_hash,last_trigger_build_reason,create_time) values ('"+TABLE_NAME+"','symmetric',1,'"+col+"' , '"+pk+"','','','',1,'T',current_timestamp)");
                ResultSet rs = s.getGeneratedKeys();
                rs.next();
                TriggerHistory audit = new TriggerHistory(TABLE_NAME, pk, col);
                audit.setTriggerHistoryId(rs.getInt(1));
                return audit;
            }
        });
    }

    protected DataSource getDataSource()
    {
        return (DataSource) getBeanFactory().getBean("dataSource");
    }

    protected JdbcTemplate getJdbcTemplate()
    {
        return new JdbcTemplate(getDataSource());
    }
    
    class ExpectMaster5000
    {
        StringWriter base;
        BufferedWriter writer;
        
        ExpectMaster5000()
        {   
            base = new StringWriter(); 
            writer = new BufferedWriter(base);
        }
        
        void location(String location) throws IOException
        {
            writeCSV(CsvConstants.NODEID);
            writer.write(location);
            writer.newLine();
        }
        
        void batchBegin(String batchId) throws IOException
        {
            writeCSV(CsvConstants.BATCH);
            writer.write(batchId);
            writer.newLine();
        }
        
        void batchEnd(String batchId) throws IOException
        {
            writeCSV(CsvConstants.COMMIT);
            writer.write(batchId);
            writer.newLine();
        }
        
        void table(String tableName, String pk, String cols) throws IOException 
        {
            writeCSV(CsvConstants.TABLE);
            writer.write(tableName);
            writer.newLine();
            writeCSV(CsvConstants.KEYS);
            writer.write(pk);
            writer.newLine();
            writeCSV(CsvConstants.COLUMNS);
            writer.write(cols);
            writer.newLine();
        }
        
        void insert(String data) throws IOException 
        {
            writeCSV(CsvConstants.INSERT);
            writer.write(data);
            writer.newLine();
        }
        
        void update(String rowData, String pk) throws IOException
        {
            writeCSV(CsvConstants.UPDATE);
            writeCSV(rowData);
            writer.write(pk);
            writer.newLine();
        }
        
        void delete(String pk) throws IOException
        {
            writeCSV(CsvConstants.DELETE);
            writer.write(pk);
            writer.newLine();
        }     
        
        void table(String t) throws IOException
        {
            writeCSV(CsvConstants.TABLE);
            writer.write(t);
            writer.newLine();
        }
        
        private void writeCSV(String constant) throws IOException
        {
            writer.write(constant);
            writer.write(", ");
        }
        
        @Override
        public String toString()
        {
            try
            {
                writer.flush();
                return base.toString();
            }
            catch (IOException e)
            {
                Assert.fail("", e);
            }
            return null;
        }
        
        @Override
        public boolean equals(Object other) 
        {
            if (other instanceof String)
            {
                try
                {
                    String s = (String) other;
                    writer.flush();
                    String out = base.toString();
                    return out.equals(s);
                }
                catch (IOException e)
                {
                    Assert.fail("", e);
                }
            }
            
            return false;
        }
    }
    
    class TestData
    {
        String table;
        String rowData;
        String key;
        long dataId;
        String keyColumns;
        String columns;
        
        TestData(long dataId, String table, String rowData, String key, String keyColumns, 
            String columns)
        {
            this.dataId = dataId;
            this.table = table;
            this.rowData = rowData;
            this.key = key;
            this.keyColumns = keyColumns;
            this.columns = columns;
        }
    }

}
