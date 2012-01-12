package org.jumpmind.symmetric.io.data.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.FileIoResource;
import org.jumpmind.symmetric.io.IoResource;
import org.jumpmind.symmetric.io.MemoryIoResource;
import org.jumpmind.symmetric.io.data.Batch;

public class FileCsvDataWriter extends AbstractCsvDataWriter {

    protected FileChannel channel = null;

    protected File dir;

    protected long memoryThresholdInBytes;

    protected File currentBatchFile;

    // TODO pool string builders?
    protected StringBuilder currentBatchBuffer;

    protected IoResource lastBatch;

    public FileCsvDataWriter(File dir, long memoryThresholdInBytes,
            ICsvDataWriterListener... listeners) {
        this(dir, memoryThresholdInBytes, toList(listeners));
    }

    public FileCsvDataWriter(File dir, long memoryThresholdInBytes,
            List<ICsvDataWriterListener> listeners) {
        super(listeners);
        this.dir = dir;
        this.memoryThresholdInBytes = memoryThresholdInBytes;
    }

    public static List<ICsvDataWriterListener> toList(ICsvDataWriterListener... listeners) {
        ArrayList<ICsvDataWriterListener> list = new ArrayList<ICsvDataWriterListener>(
                listeners.length);
        for (ICsvDataWriterListener l : listeners) {
            list.add(l);
        }
        return list;
    }

    protected void initFile(Batch batch) {
        try {
            this.currentBatchFile = new File(dir, toFileName(batch, true));
            if (!this.currentBatchFile.getParentFile().exists()) {
                this.currentBatchFile.getParentFile().mkdirs();
            }
            if (this.currentBatchFile.exists()) {
                this.currentBatchFile.delete();
            }
            this.channel = new FileOutputStream(this.currentBatchFile).getChannel();
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    @Override
    protected void notifyEndBatch(Batch batch, ICsvDataWriterListener listener) {
        listener.end(batch, this.lastBatch);
    }

    @Override
    protected void endBatch(Batch batch) {
        closeFile(batch);
        flushNodeId = true;
    }

    protected void closeFile(Batch batch) {
        if (channel != null) {
            try {
                this.channel.close();
                this.channel = null;
            } catch (IOException ex) {
                // do nothing. it doesn't really matter.
            }
        }

        if (this.currentBatchFile != null && this.currentBatchFile.exists()) {
            File targetFile = new File(dir, toFileName(batch, false));
            targetFile.delete();
            this.currentBatchFile.renameTo(targetFile);
            this.lastBatch = new FileIoResource(targetFile);
            this.currentBatchFile = null;

        } else if (this.currentBatchBuffer != null) {
            try {
                this.lastBatch = new MemoryIoResource(this.currentBatchBuffer.toString().getBytes(
                        "UTF-8"));
                this.currentBatchBuffer = null;
            } catch (IOException ex) {
                throw new IoException(ex);
            }
        }
    }

    protected String toFileName(Batch batch, boolean extracting) {
        return (batch.getSourceNodeId() != null ? (batch.getSourceNodeId() + "-") : "")
                + Long.toString(batch.getBatchId()) + ".csv" + (extracting ? ".extracting" : "");
    }

    @Override
    protected void print(Batch batch, String data) {
        int currentSize = (currentBatchBuffer != null ? currentBatchBuffer.length() : 0)
                + data.length();
        if (memoryThresholdInBytes > 0 && currentBatchBuffer == null
                && currentSize < memoryThresholdInBytes) {
            currentBatchBuffer = new StringBuilder();
        }

        if (currentBatchBuffer != null) {
            currentBatchBuffer.append(data);
        } else {
            if (currentBatchBuffer != null) {
                data = currentBatchBuffer.toString();
                currentBatchBuffer = null;
            }
            if (channel == null) {
                initFile(batch);
            }
            try {
                byte[] bytes = data.getBytes("UTF-8");
                ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 0, bytes.length);
                channel.write(byteBuffer);
            } catch (IOException ex) {
                throw new IoException(ex);
            }
        }
    }
    
    public IoResource getLastBatch() {
        return lastBatch;
    }

}
