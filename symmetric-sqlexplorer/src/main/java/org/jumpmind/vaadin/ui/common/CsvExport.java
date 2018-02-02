package org.jumpmind.vaadin.ui.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.ui.UI;

public class CsvExport {

    protected IDataProvider gridData = null;
    protected String fileName;
    protected String title;

    protected StringBuffer cellData;
    protected final String csvMimeContentType = "text/csv";

    final Logger log = LoggerFactory.getLogger(getClass());

    public CsvExport(final IDataProvider gridData) {
        this(gridData, null, null);
    }

    public CsvExport(final IDataProvider gridData, String fileName) {
        this(gridData, fileName, null);
    }

    public CsvExport(final IDataProvider gridData, String fileName, String title) {
        this.gridData = gridData;
        init(fileName, title);
    }

    public void init(String fileName, String title) {
        if (fileName == null || fileName.isEmpty() || !fileName.endsWith(".csv")) {
            this.fileName = "GridExport.csv";
        } else {
            this.fileName = fileName;
        }

        if (title == null || title.isEmpty()) {
            this.title = "";
        } else {
            this.title = title;
        }
        this.cellData = new StringBuffer();
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void export() {
        convertToCsv();
        sendCsvToUser();
    }

    public void convertToCsv() {
        addTitle();
        addHeaders();
        Iterator<?> iterator = gridData.getRowItems().iterator();
        while (iterator.hasNext()) {
            Object rowItem = iterator.next();
            Iterator<?> columnIterator = gridData.getColumns().iterator();
            while (columnIterator.hasNext()) {
                Object col = columnIterator.next();
                String value = String.valueOf(gridData.getCellValue(rowItem, col));
                if (columnIterator.hasNext()) {
                    cellData.append(value + ",");
                } else {
                    cellData.append(value + "\n");
                }
            }
        }
    }

    public void addTitle() {
        if (this.title != null && !this.title.equals("")) {
            cellData.append(title + ",\n");
        }
    }

    public void addHeaders() {
        if (gridData.isHeaderVisible()) {
            Iterator<?> iterator = gridData.getColumns().iterator();
            while (iterator.hasNext()) {
                Object col = iterator.next();
                String value = gridData.getHeaderValue(col);
                if (iterator.hasNext()) {
                    cellData.append(value + ",");
                } else {
                    cellData.append(value + "\n");
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    public void sendCsvToUser() {
        FileOutputStream outStream = null;
        File file = null;
        try {
            String prefix = fileName.substring(0, fileName.length() - 4);
            file = File.createTempFile(prefix, ".csv");
            outStream = new FileOutputStream(file);
            outStream.write(cellData.toString().getBytes());

            ExportFileDownloader downloader = new ExportFileDownloader(fileName, csvMimeContentType, file);
            UI.getCurrent().getPage().open(downloader, "Download", false);
        } catch (Exception e) {
            log.error("", e);
        } finally {
            try {
                file.delete();
                outStream.close();
            } catch (IOException e) {
                log.error("Problem closing File Stream", e);
            }
        }
    }
}