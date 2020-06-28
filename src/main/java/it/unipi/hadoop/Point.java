package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Point implements Writable {
  private List<DoubleWritable> coordinates;

  Point() {
    coordinates = new ArrayList<DoubleWritable>();
  }

  Point(int n) {
    coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < n; i++) {
      coordinates.add(new DoubleWritable(0.0));
    }
  }

  Point(List<DoubleWritable> coordinatesList) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (DoubleWritable p : coordinatesList) {
        this.coordinates.add(p);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(coordinates.size());

    for (DoubleWritable p : coordinates) {
        out.writeDouble(p.get());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < size; i++) {
        coordinates.add(new DoubleWritable(in.readDouble()));
    }
  }

  @Override
  public String toString() {
    String elements = "";

    for (DoubleWritable element : coordinates) {
        elements += element.get() + ";";
    }

    return elements;
  }

  List<DoubleWritable> getCoordinates() {
    return coordinates;
  }
}
