import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;

public class MyFilter extends DatabaseWriterFilterAdapter {

    @Override
    public boolean beforeWrite(DataContext context, Table table, CsvData data) {
        if (table.getName().equalsIgnoreCase("CREDIT_CARD_TENDER")
                && data.getDataEventType().equals(DataEventType.INSERT)) {
            String[] parsedData = data.getParsedData(CsvData.ROW_DATA);
            // blank out credit card number
            parsedData[table.getColumnIndex("CREDIT_CARD_NUMBER")] = null;
        }
        return true;
    }
}
