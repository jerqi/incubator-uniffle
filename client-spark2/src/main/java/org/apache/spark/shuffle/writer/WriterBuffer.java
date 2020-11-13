package org.apache.spark.shuffle.writer;

import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.serializer.SerializationStream;

public class WriterBuffer {

    private SerializationStream serializeStream;
    private Output output;

    public WriterBuffer(SerializationStream serializeStream, Output output) {
        this.serializeStream = serializeStream;
        this.output = output;
    }

    public SerializationStream getSerializeStream() {
        return serializeStream;
    }

    public void setSerializeStream(SerializationStream serializeStream) {
        this.serializeStream = serializeStream;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }
}
