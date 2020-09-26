package average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AverageTuple implements Writable {

    private long count = 0L;
    private double length = 0.0;

    @Override
    public void readFields(DataInput in) throws IOException {
        this.count = in.readLong();
        this.length = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.count);
        out.writeDouble(this.length);
    }

    public void add(AverageTuple tuple) {
        setCount(this.count + tuple.getCount());
        setLength(this.length + tuple.getLength());
    }

    public long getCount() { return this.count; }
    public void setCount(long count) { this.count = count; }
    public double getLength() { return this.length; }
    public void setLength(double length) { this.length = length; }

    public double getAvg() { return this.length/this.count; }

    @Override
    public String toString() {
        return "count: "+ this.count +"\t length: "+ this.length +" \t avg: "+ this.getAvg();
    } 
}