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

    this.id = new IntWritable(id.get());
  }

  public IntWritable getId() {
    return this.id;
  }

  public void setId(IntWritable value) {
    this.id = new IntWritable(value.get());
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
    return this.getId().get() + ";" + super.toString();
  }

  @Override
  public int compareTo(Centroid o) {
    return Integer.compare(this.getId().get(), o.getId().get());
  }

  public void add(Point point) {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();

    for (int i = 0; i < lenght; i++) {
      DoubleWritable newValue = new DoubleWritable(this.getCoordinates().get(i).get() + pointCoordinates.get(i).get());

      this.getCoordinates().set(i, newValue);
    }
  }

  public void mean(int n) {
    int lenght = this.getCoordinates().size();

    for (int i = 0; i < lenght; i++) {
      DoubleWritable newValue = new DoubleWritable(this.getCoordinates().get(i).get() / n);

      this.getCoordinates().set(i, newValue);
    }
  }

  public Double findEuclideanDistance(Point point) {
    int lenght = point.getCoordinates().size();
    List<DoubleWritable> pointCoordinates = point.getCoordinates();
    Double sum = 0.0;

    for (int i = 0; i < lenght; i++) {
        DoubleWritable difference = new DoubleWritable(this.getCoordinates().get(i).get() - pointCoordinates.get(i).get());

        sum += Math.pow(difference.get(), 2);
    }

    return Math.sqrt(sum);
  }

  public static Centroid copy(final Centroid old) {
    return new Centroid(old.getId(), old.getCoordinates());
  }
}
