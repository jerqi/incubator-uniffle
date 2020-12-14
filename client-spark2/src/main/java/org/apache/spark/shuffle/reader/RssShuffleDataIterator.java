package org.apache.spark.shuffle.reader;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.FileBasedShuffleReadHandler;
import com.tencent.rss.storage.FileBasedShuffleSegment;
import com.tencent.rss.storage.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

    private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

    private Map<String, FileBasedShuffleReadHandler> pathToHandler = Maps.newHashMap();
    private Map<Long, Queue<ShuffleSegment>> blockIdToSegments = Maps.newHashMap();
    private Queue<Long> blockIdQueue = Queues.newLinkedBlockingQueue();
    private Set<Long> processedBlockIds = Sets.newHashSet();
    private Iterator<Tuple2<Object, Object>> recordsIterator = null;
    private Set<Long> expectedBlockIds;
    private SerializerInstance serializerInstance;
    private int indexReadLimit;

    public RssShuffleDataIterator(int indexReadLimit, Serializer serializer, Set<Long> expectedBlockIds) {
        if (indexReadLimit < 1) {
            throw new RuntimeException("Invalid indexReadLimit for RssShuffleDataIterator:" + indexReadLimit);
        }
        this.indexReadLimit = indexReadLimit;
        this.serializerInstance = serializer.newInstance();
        this.expectedBlockIds = expectedBlockIds;
    }

    public void init(String basePath, Configuration hadoopConf) {
        FileSystem fs;
        Path baseFolder = new Path(basePath);
        try {
            fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
        } catch (IOException ioe) {
            throw new RuntimeException("Can't get FileSystem for " + basePath);
        }

        FileStatus[] indexFiles;
        String failedGetIndexFileMsg = "No index file found in  " + basePath;
        try {
            // get all index files
            indexFiles = fs.listStatus(baseFolder,
                    file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
        } catch (Exception e) {
            throw new RuntimeException(failedGetIndexFileMsg);
        }

        if (indexFiles == null || indexFiles.length == 0) {
            throw new RuntimeException(failedGetIndexFileMsg);
        }
        for (FileStatus status : indexFiles) {
            String fileNamePrefix = getFileNamePrefix(status.getPath().getName());
            try {
                pathToHandler.put(fileNamePrefix,
                        new FileBasedShuffleReadHandler(basePath, fileNamePrefix, hadoopConf));
            } catch (Exception e) {
                LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
            }
        }

        Map<String, List<FileBasedShuffleSegment>> pathToSegments = readAllIndexSegments();
        updateBlockIdToSegments(pathToSegments);
    }

    /**
     * Read all index files, and get all FileBasedShuffleSegment for every index file
     */
    public Map<String, List<FileBasedShuffleSegment>> readAllIndexSegments() {
        Map<String, List<FileBasedShuffleSegment>> pathToSegments = Maps.newHashMap();
        for (Entry<String, FileBasedShuffleReadHandler> entry : pathToHandler.entrySet()) {
            String path = entry.getKey();
            try {
                FileBasedShuffleReadHandler handler = entry.getValue();
                List<FileBasedShuffleSegment> segments = handler.readIndex(indexReadLimit);
                List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
                while (!segments.isEmpty()) {
                    allSegments.addAll(segments);
                    segments = handler.readIndex(indexReadLimit);
                }
                pathToSegments.put(path, allSegments);
            } catch (Exception e) {
                LOG.warn("Can't read index segments for " + path, e);
            }
        }
        return pathToSegments;
    }

    /**
     * Parse every FileBasedShuffleSegment, and check if have all required blockIds
     */
    public void updateBlockIdToSegments(Map<String, List<FileBasedShuffleSegment>> pathToSegments) {
        for (Entry<String, List<FileBasedShuffleSegment>> entry : pathToSegments.entrySet()) {
            String path = entry.getKey();
            for (FileBasedShuffleSegment segment : entry.getValue()) {
                long blockId = segment.getBlockId();
                if (expectedBlockIds.contains(blockId)) {
                    if (blockIdToSegments.get(blockId) == null) {
                        blockIdToSegments.put(blockId, Queues.newArrayDeque());
                        blockIdQueue.add(blockId);
                    }
                    blockIdToSegments.get(blockId).add(new ShuffleSegment(path, segment));
                }
            }
        }
        Set<Long> copy = Sets.newHashSet(expectedBlockIds);
        copy.removeAll(blockIdToSegments.keySet());
        if (copy.size() > 0) {
            throw new RuntimeException("Can't find blockIds " + copy.toString() + "");
        }
    }

    public byte[] readShuffleData() {
        // get next blockId
        Long blockId = blockIdQueue.poll();
        if (blockId == null) {
            return null;
        }
        // get segment queue
        Queue<ShuffleSegment> segmentQueue = blockIdToSegments.get(blockId);
        ShuffleSegment segment = segmentQueue.poll();
        if (segment == null) {
            throw new RuntimeException("Can't read data with blockId:" + blockId);
        }
        byte[] data = null;
        long expectedLength = -1;
        long expectedCrc = -1;
        long actualCrc = -1;
        boolean readSuccess = false;
        while (!readSuccess) {
            try {
                FileBasedShuffleSegment fss = segment.segment;
                data = pathToHandler.get(segment.path).readData(fss);
                expectedLength = fss.getLength();
                expectedCrc = fss.getCrc();
                readSuccess = true;
                actualCrc = ChecksumUtils.getCrc32(data);
            } catch (Exception e) {
                // read failed for current segment, try next one if possible
                LOG.warn("Can't read data from " + segment.path + "["
                        + segment.segment + "] for blockId[" + blockId + "]", e);
                segment = segmentQueue.poll();
                if (segment == null) {
                    throw new RuntimeException("Can't read data with blockId:" + blockId);
                }
            }
        }
        if (data == null) {
            throw new RuntimeException("Can't read data for blockId[" + blockId + "]");
        } else if (data.length != expectedLength) {
            throw new RuntimeException("Unexpected data length for blockId[" + blockId
                    + "], expected:" + expectedLength + ", actual:" + data.length);
        }
        if (expectedCrc != actualCrc) {
            throw new RuntimeException("Unexpected crc value for blockId[" + blockId
                    + "], expected:" + expectedCrc + ", actual:" + actualCrc);
        }
        processedBlockIds.add(blockId);
        return data;
    }

    public Iterator<Tuple2<Object, Object>> createKVIterator(byte[] data) {
        Input deserializationInput = new Input(data, 0, data.length);
        DeserializationStream ds = serializerInstance.deserializeStream(deserializationInput);
        return ds.asKeyValueIterator();
    }

    private String getFileNamePrefix(String fileName) {
        int point = fileName.lastIndexOf(".");
        return fileName.substring(0, point);
    }

    @VisibleForTesting
    protected Queue<Long> getBlockIdQueue() {
        return blockIdQueue;
    }

    @Override
    public boolean hasNext() {
        if (recordsIterator == null || !recordsIterator.hasNext()) {
            // read next segment
            byte[] data = readShuffleData();
            if (data != null) {
                // create new iterator for shuffle data
                recordsIterator = createKVIterator(data);
            } else {
                // finish reading records, close related reader and check data consistent
                closeReader();
                checkProcessedBlockIds();
                return false;
            }
        }
        return recordsIterator.hasNext();
    }

    private void checkProcessedBlockIds() {
        Set<Long> missingBlockIds = Sets.difference(expectedBlockIds, processedBlockIds);
        if (expectedBlockIds.size() != processedBlockIds.size() || !missingBlockIds.isEmpty()) {
            throw new RuntimeException("Blocks read inconsistent: expected " + expectedBlockIds.toString()
                    + ", actual " + processedBlockIds.toString());
        }
    }

    private void closeReader() {
        for (Entry<String, FileBasedShuffleReadHandler> entry : pathToHandler.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.warn("Can't close reader successfully for " + entry.getKey(), e);
            }
        }
    }

    @Override
    public Product2<K, C> next() {
        return (Product2<K, C>) recordsIterator.next();
    }

    @Override
    public String toString() {
        return "test";
    }

    class ShuffleSegment {

        String path;
        FileBasedShuffleSegment segment;

        ShuffleSegment(String path, FileBasedShuffleSegment segment) {
            this.path = path;
            this.segment = segment;
        }
    }
}

