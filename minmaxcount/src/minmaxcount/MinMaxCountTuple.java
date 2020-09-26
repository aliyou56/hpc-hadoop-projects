package minmaxcount;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxCountTuple implements Writable {

    private static final ZoneId ZONE_ID = ZoneId.systemDefault();

    private LocalDateTime min = LocalDateTime.now();
    private LocalDateTime max = LocalDateTime.now();
    // private Date min = new Date();
    // private Date max = new Date();
    private long count = 0;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    // private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    @Override
    public void readFields(DataInput in) throws IOException {
        this.min = Instant.ofEpochMilli(in.readLong()).atZone(ZONE_ID).toLocalDateTime();
        this.max = Instant.ofEpochMilli(in.readLong()).atZone(ZONE_ID).toLocalDateTime();
        
        // this.min = new Date(in.readLong());
        // this.max = new Date(in.readLong());
        this.count = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.min.atZone(ZONE_ID).toInstant().toEpochMilli());
        out.writeLong(this.max.atZone(ZONE_ID).toInstant().toEpochMilli());
        // out.writeLong(this.min.getTime());
        // out.writeLong(this.max.getTime());
        out.writeLong(this.count);
    }

    public String toString() {
        return this.min.format(formatter) + "\t" + this.max.format(formatter) + "\t" + this.count;
        // return sdf.format(this.min) +"\t"+ sdf.format(this.max) +"\t"+ this.count;
    }

    public LocalDateTime getMin() { return this.min; }
    public void setMin(LocalDateTime min) { this.min = min; }
    public LocalDateTime getMax() { return this.max; }
    public void setMax(LocalDateTime max) { this.max = max; }
    public long getCount() { return this.count; }
    public void setCount(long count) { this.count = count; }
    // public Date getMin() { return this.min; }
    // public Date getMax() { return this.max; }
    // public void setMin(Date min) { this.min = min; }
    // public void setMax(Date max) { this.max = max; }
}