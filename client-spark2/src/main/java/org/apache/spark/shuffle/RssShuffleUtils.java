package org.apache.spark.shuffle;

import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.spark.io.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleUtils.class);

  public static byte[] compressDataOrigin(CompressionCodec compressionCodec, byte[] data) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
    OutputStream os = compressionCodec.compressedOutputStream(baos);
    try {
      os.write(data);
      os.flush();
    } catch (Exception e) {
      LOG.error("Fail to compress shuffle data", e);
      throw new RuntimeException(e);
    } finally {
      try {
        os.close();
      } catch (Exception e) {
        LOG.warn("Can't close compression output stream, resource leak", e);
      }
    }
    return baos.toByteArray();
  }

  public static byte[] decompressDataOrigin(CompressionCodec compressionCodec, byte[] data, int compressionBlockSize) {
    if (data == null || data.length == 0) {
      LOG.warn("Empty data is found when do decompress");
      return null;
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    InputStream is = compressionCodec.compressedInputStream(bais);
    List<byte[]> dataBlocks = Lists.newArrayList();
    int readSize = 0;
    int lastReadSize = 0;
    boolean shouldEnd = false;
    do {
      byte[] block = new byte[compressionBlockSize];
      lastReadSize = readSize;
      try {
        readSize = is.read(block, 0, compressionBlockSize);
      } catch (Exception e) {
        LOG.error("Fail to decompress shuffle data", e);
        throw new RuntimeException(e);
      }
      if (readSize > -1) {
        dataBlocks.add(block);
      }
      if (shouldEnd && readSize > -1) {
        String errorMsg = "Fail to decompress shuffle data, it may be caused by incorrect compression buffer";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
      if (readSize < compressionBlockSize) {
        shouldEnd = true;
      }
    } while (readSize > -1);
    int uncompressLength = (dataBlocks.size() - 1) * compressionBlockSize + lastReadSize;
    byte[] uncompressData = new byte[uncompressLength];
    for (int i = 0; i < dataBlocks.size() - 1; i++) {
      System.arraycopy(dataBlocks.get(i), 0, uncompressData,
          i * compressionBlockSize, compressionBlockSize);
    }
    byte[] lastBlock = dataBlocks.get(dataBlocks.size() - 1);
    System.arraycopy(lastBlock, 0,
        uncompressData, (dataBlocks.size() - 1) * compressionBlockSize, lastReadSize);
    return uncompressData;
  }

  public static byte[] compressData(byte[] data) {
    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    return compressor.compress(data);
  }

  public static byte[] decompressData(byte[] data, int uncompressLength) {
    LZ4FastDecompressor fastDecompressor = LZ4Factory.fastestInstance().fastDecompressor();
    byte[] uncompressData = new byte[uncompressLength];
    fastDecompressor.decompress(data, 0, uncompressData, 0, uncompressLength);
    return uncompressData;
  }
}
