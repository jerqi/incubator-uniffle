package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.ShuffleStorageWriteHandler;
import com.tencent.rss.storage.StorageType;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleEngine {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleEngine.class);

    private String appId;
    private String shuffleId;
    private int startPartition;
    private int endPartition;
    private ShuffleBuffer buffer;
    private ShuffleStorageWriteHandler writer;

    public ShuffleEngine(String appId, String shuffleId, int startPartition, int endPartition) {
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.startPartition = startPartition;
        this.endPartition = endPartition;
    }

    public StatusCode init() throws IOException, IllegalStateException {
        synchronized (this) {
            buffer = BufferManager.instance().getBuffer(startPartition, endPartition);
            if (buffer == null) {
                return StatusCode.NO_BUFFER;
            }

            if (ShuffleTaskManager.instance().storageType == StorageType.FILE) {
                writer = new FileBasedShuffleWriteHandler("", "", null);
            }

            return StatusCode.SUCCESS;
        }
    }

    public StatusCode write(List<ShufflePartitionedData> shuffleData) throws IOException, IllegalStateException {
        synchronized (this) {
            if (buffer == null) {
                // is committed
                buffer = BufferManager.instance().getBuffer(startPartition, endPartition);

                if (buffer == null) {
                    return StatusCode.NO_BUFFER;
                }
            }

            for (ShufflePartitionedData data : shuffleData) {
                StatusCode ret = write(data);
                if (ret != StatusCode.SUCCESS) {
                    return ret;
                }
            }

            return StatusCode.SUCCESS;
        }
    }

    private StatusCode write(ShufflePartitionedData data) throws IOException, IllegalStateException {
        StatusCode ret = buffer.append(data);
        if (ret != StatusCode.SUCCESS) {
            return ret;
        }

        if (buffer.full()) {
            flush();
        }

        return StatusCode.SUCCESS;
    }

    public StatusCode flush() throws IOException, IllegalStateException {
        synchronized (this) {
            for (int partition = startPartition; partition <= endPartition; ++partition) {
                writer.write(buffer.getBlocks(partition));
            }

            buffer.clear();
            buffer.setSize(0);

            return StatusCode.SUCCESS;
        }
    }

    private void clear() {
        if (buffer != null) {
            buffer.clear();
        }
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getShuffleId() {
        return shuffleId;
    }

    public void setShuffleId(String shuffleId) {
        this.shuffleId = shuffleId;
    }

    public int getStartPartition() {
        return startPartition;
    }

    public void setStartPartition(int startPartition) {
        this.startPartition = startPartition;
    }

    public int getEndPartition() {
        return endPartition;
    }

    public void setEndPartition(int endPartition) {
        this.endPartition = endPartition;
    }

}
