package org.jumpmind.symmetric.core.process.csv;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.process.IDataFilter;

public class FileCsvDataWriter extends AbstractCsvDataWriter {

    protected FileChannel channel = null;

    protected File dir;

    protected File extractingFile;

    public FileCsvDataWriter(File dir, IDataFilter... filters) {
        super(filters);
        dir.mkdirs();
        this.dir = dir;
    }

    @Override
    public void startBatch(Batch batch) {
        try {
            close();
            extractingFile = new File(dir, toFileName(batch, true));
            if (extractingFile.exists()) {
                extractingFile.delete();
            }
            channel = new FileOutputStream(extractingFile).getChannel();
            super.startBatch(batch);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    protected String toFileName(Batch batch, boolean extracting) {
        return (batch.getSourceNodeId() != null ? (batch.getSourceNodeId() + "-") : "")
                + Long.toString(batch.getBatchId()) + ".csv" + (extracting ? ".extracting" : "");
    }

    public void close() {
        if (channel != null) {
            try {
                channel.close();
                channel = null;
            } catch (IOException ex) {
                // do nothing. it doesn't really matter.
            }
        }

        if (extractingFile != null && extractingFile.exists()) {
            File targetFile = new File(dir, toFileName(batch, false));
            targetFile.delete();
            extractingFile.renameTo(targetFile);
        }
    }

    @Override
    protected void print(String data) {
        try {            
            byte[] bytes = data.getBytes("UTF-8");
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);
            channel.write(byteBuffer);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }
}
