package org.apache.spark.shuffle.writer;

public class BufferManagerOptions {

    private int individualBufferSize;
    private int individualBufferMax;
    private int bufferSpillThreshold;

    public BufferManagerOptions() {
    }

    public BufferManagerOptions(int individualBufferSize, int individualBufferMax, int bufferSpillThreshold) {
        this.individualBufferSize = individualBufferSize;
        this.individualBufferMax = individualBufferMax;
        this.bufferSpillThreshold = bufferSpillThreshold;
    }

    public int getIndividualBufferSize() {
        return individualBufferSize;
    }

    public void setIndividualBufferSize(int individualBufferSize) {
        this.individualBufferSize = individualBufferSize;
    }

    public int getIndividualBufferMax() {
        return individualBufferMax;
    }

    public void setIndividualBufferMax(int individualBufferMax) {
        this.individualBufferMax = individualBufferMax;
    }

    public int getBufferSpillThreshold() {
        return bufferSpillThreshold;
    }

    public void setBufferSpillThreshold(int bufferSpillThreshold) {
        this.bufferSpillThreshold = bufferSpillThreshold;
    }
}
