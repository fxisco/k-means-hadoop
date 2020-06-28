package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Centroid extends Point {
  private IntWritable index;

  Centroid() {
    super();
  }

  Centroid(int n) {
      super(n);
  }

  Centroid(IntWritable id, List<DoubleWritable> coordinates) {
    super(coordinates);

    index = id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(index.get());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    index = new IntWritable(in.readInt());
  }
}
