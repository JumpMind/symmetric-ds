package org.jumpmind.vaadin.ui.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.vaadin.flow.server.StreamResource;

public class ExportFileDownloader extends StreamResource {
    private static final long serialVersionUID = 1L;
    protected String contentType;
    protected String fileName;

    public ExportFileDownloader(final String fileName, final String contentType, final File file) throws FileNotFoundException {
        //super(new FileStreamSource(file),fileName);
        super(fileName, () -> {
            try {
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
                return null;
            }
        });
        this.contentType = contentType;
        this.fileName = fileName;
    }
    /*
    @Override
    public DownloadStream getStream() {
        DownloadStream download = new DownloadStream(super.getStreamSource().getStream(),contentType,fileName);
        download.setCacheTime(2000);
        return download;
    }

    public static class FileStreamSource implements StreamResource.StreamSource {

        private static final long serialVersionUID = 1L;
        private FileInputStream stream;

        public FileStreamSource(File downloadFile) throws FileNotFoundException {
            stream = new FileInputStream(downloadFile);
        }

        @Override
        public InputStream getStream() {
            return stream;
        }

    }*/
}