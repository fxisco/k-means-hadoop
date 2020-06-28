package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Centroid> {
  private List<DoubleWritable> coordinates;

  Point() {
    this.coordinates = new ArrayList<DoubleWritable>();
  }

  Point(final int n) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < n; i++) {
      this.coordinates.add(new DoubleWritable(0.0));
    }
  }

  Point(final List<DoubleWritable> coordinatesList) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (final DoubleWritable p : coordinatesList) {
        this.coordinates.add(p);
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(coordinates.size());

    for (final DoubleWritable p : coordinates) {
        out.writeDouble(p.get());
    }
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final int size = in.readInt();
    coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < size; i++) {
        coordinates.add(new DoubleWritable(in.readDouble()));
    }
  }

  @Override
  public String toString() {
    String elements = "";

    for (final DoubleWritable element : coordinates) {
        elements += element.get() + ";";
    }

    return elements;
  }

  void setCoordinates(final List<DoubleWritable> newCoordinates) {
    this.coordinates = newCoordinates;
  }

  List<DoubleWritable> getCoordinates() {
    return coordinates;
  }

  @Override
  public int compareTo(Centroid o) {
    return 0;
  }
}
