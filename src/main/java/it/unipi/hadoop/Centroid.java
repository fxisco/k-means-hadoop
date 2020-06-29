package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Centroid extends Point {
  private IntWritable id;

  Centroid() {
    super();
  }

  Centroid(int n) {
      super(n);
  }

  Centroid(IntWritable id, List<DoubleWritable> coordinates) {
    super(coordinates);

    this.id = id;
  }

  public IntWritable getId() {
    return this.id;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(this.getId().get());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    this.id = new IntWritable(in.readInt());
  }

  @Override
  public String toString() {
    return this.id + ";" + super.toString();
  }

  @Override
  public int compareTo(Centroid o) {
    if (this.getId().get() == o.getId().get()) {
      return 0;
    }

    return 1;
  }

  public void add(Point point) {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();

    for (int i = 0; i < lenght; i++) {
      DoubleWritable newValue = new DoubleWritable(this.getCoordinates().get(i).get() + pointCoordinates.get(i).get());

      this.getCoordinates().set(i, newValue);
    }
  }

  public Double findEuclideanDistance(Point point) {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();
    Double sum = 0.0;

    for (int i = 0; i < lenght; i++) {
        sum += Math.pow(this.getCoordinates().get(i).get() - pointCoordinates.get(i).get(), 2);
    }

    return Math.sqrt(sum);
  }
}
