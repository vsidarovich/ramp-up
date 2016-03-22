package com.epam.bigdata.second.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vsfmqueen on 10/4/15.
 */
public class RowNumberWritable implements Writable {
    private Text value;
    private long count;
    private int partition;

    private static final Text EMPTY_STRING = new Text("");

    public void setValue(Text value) {
        this.value = value;
        if (value.getLength() == 0) {
            this.count = 0;
            this.partition = 0;
        }
    }

    public void setCounter(int partition, long count) {
        this.value = EMPTY_STRING;
        this.partition = partition;
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public int getPartition() {
        return partition;
    }

    public Text getValue() {
        return value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value.write(out);
        if (value.getLength() == 0) {
            out.writeInt(partition);
            out.writeLong(count);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (value == null) {
            value = new Text();
        }
        value.readFields(in);

        if (value.getLength() == 0) {
            partition = in.readInt();
            count = in.readLong();
        } else {
            partition = 0;
            count = 0;
        }
    }
}
