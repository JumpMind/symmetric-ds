package org.jumpmind.vaadin.ui.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.ui.UI;

public class ExcelExport {

    protected IDataProvider gridData = null;

    protected final HSSFWorkbook workbook;
    protected Sheet sheet;

    protected String sheetName;
    protected String fileName;
    protected String title;

    protected final String excelMimeContentType = "application/vnd.ms-excel";

    final Logger log = LoggerFactory.getLogger(getClass());

    public ExcelExport(final IDataProvider gridData) {
        this(gridData, new HSSFWorkbook(), null, null);
    }

    public ExcelExport(final IDataProvider gridData, String sheetName) {
        this(gridData, new HSSFWorkbook(), sheetName, null);
    }

    public ExcelExport(final IDataProvider gridData, String sheetName, String title) {
        this(gridData, new HSSFWorkbook(), sheetName, title, null);
    }

    public ExcelExport(final IDataProvider gridData, String sheetName, String title, String fileName) {
        this(gridData, new HSSFWorkbook(), sheetName, title, fileName);
    }

    public ExcelExport(final IDataProvider gridData, HSSFWorkbook wkbk) {
        this(gridData, wkbk, null, null, null);
    }

    public ExcelExport(final IDataProvider gridData, HSSFWorkbook wkbk, String sheetName) {
        this(gridData, wkbk, sheetName, null, null);
    }

    public ExcelExport(final IDataProvider gridData, HSSFWorkbook wkbk, String sheetName, String title) {
        this(gridData, wkbk, sheetName, title, null);
    }

    public ExcelExport(final IDataProvider gridData, HSSFWorkbook wkbk, String sheetName, String title, String fileName) {
        this.gridData = gridData;
        this.title = title;
        this.workbook = wkbk;
        init(sheetName, title, fileName);
    }

    private void init(final String sheetName, final String title, final String fileName) {
        if (sheetName == null || sheetName.isEmpty()) {
            this.sheetName = "Grid Export";
        } else {
            this.sheetName = sheetName;
        }

        if (fileName == null || fileName.isEmpty() || !fileName.endsWith(".xls")) {
            this.fileName = "GridExport.xls";
        } else {
            this.fileName = fileName;
        }

        if (title == null || title.isEmpty()) {
            this.title = "";
        } else {
            this.title = title;
        }

        this.sheet = workbook.createSheet(this.sheetName);
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setSheetName(String sheet) {
        this.sheetName = sheet;
    }

    public void export() {
        convertToExcel();
        sendExcelToUser();
    }

    public void convertToExcel() {

        int rowNum = addTitle();
        rowNum = addHeaders(rowNum);
        Iterator<?> iterator = gridData.getRowItems().iterator();
        while (iterator.hasNext()) {
            Object rowItem = iterator.next();
            Row row = sheet.createRow(rowNum++);
            CellStyle style = createDefaultStyle();
            int colCount = 0;
            List<?> columns = gridData.getColumns();
            for (Object col : columns) {
                Cell cell = row.createCell(colCount++);
                Object value = gridData.getCellValue(rowItem, col);

                if (value instanceof Integer) {
                    cell.setCellValue((Integer) value);
                } else if (value instanceof Long) {
                    cell.setCellValue((Long) value);
                } else if (value instanceof Double || value instanceof BigDecimal) {
                    cell.setCellValue((Double) value);
                } else if (value instanceof Boolean) {
                    cell.setCellValue((Boolean) value);
                } else if (value instanceof Date) {
                    cell.setCellValue((Date) value);
                } else {
                    cell.setCellValue((String) value);
                }
                cell.setCellStyle(style);
            }
        }

    }

    public int addHeaders(int lastIndex) {
        if (gridData.isHeaderVisible()) {
            Row row = sheet.createRow(lastIndex++);
            int colCount = 0;
            List<?> columns = gridData.getColumns();
            CellStyle style = createHeaderStyle();
            row.setRowStyle(style);
            for (Object col : columns) {
                String caption = gridData.getHeaderValue(col);
                Cell cell = row.createCell(colCount++);
                cell.setCellValue(caption);
                cell.setCellStyle(style);
            }
        }
        return lastIndex;
    }

    public HSSFCellStyle createHeaderStyle() {
        HSSFCellStyle style = workbook.createCellStyle();
        final HSSFFont font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setBorderBottom(BorderStyle.THIN);
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setFillForegroundColor(HSSFColor.HSSFColorPredefined.PALE_BLUE.getIndex());
        return style;
    }
    
    public HSSFCellStyle createTitleStyle() {
        HSSFCellStyle style = workbook.createCellStyle();
        final HSSFFont font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setBorderBottom(BorderStyle.THICK);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        return style;
    }

    public HSSFCellStyle createDefaultStyle() {
        HSSFCellStyle style = workbook.createCellStyle();
        HSSFFont font = workbook.createFont();
        style.setFont(font);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setAlignment(HorizontalAlignment.CENTER);
        return style;
    }

    public int addTitle() {
        if (this.title == null || this.title.equals("")) {
            return 0;
        }
        Row titleRow = sheet.createRow(0);
        titleRow.setRowStyle(createTitleStyle());
        Cell titleCell = titleRow.createCell(0);
        titleCell.setCellValue(title);
        titleCell.setCellStyle(createTitleStyle());
        return 1;
    }

    @SuppressWarnings("deprecation")
    public void sendExcelToUser() {
        FileOutputStream outStream = null;
        File file = null;
        try {
            String prefix = fileName.substring(0, fileName.length() - 4);
            file = File.createTempFile(prefix, ".xls");
            outStream = new FileOutputStream(file);
            workbook.write(outStream);

            ExportFileDownloader downloader = new ExportFileDownloader(fileName, excelMimeContentType, file);
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