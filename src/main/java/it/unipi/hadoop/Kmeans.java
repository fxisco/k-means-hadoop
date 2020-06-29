package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.SequenceFile;

import java.util.ArrayList;
import java.util.List;

public class Kmeans {

  public static class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
    private final List<Centroid> centroids = new ArrayList<>();
    private final Point point = new Point();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsFilename"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroidsPath));
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();

        while (reader.next(key, value)) {
          Centroid c = new Centroid(key, value.getCoordinates());

          centroids.add(c);
        }

        reader.close();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      final int DIMENSION = Integer.parseInt(conf.get("dimension"));
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");
      int counter = 0;
      List<DoubleWritable> coordinates = new ArrayList<DoubleWritable>();

      while (itr.hasMoreTokens()) {
          if (counter == DIMENSION)
            break;

          double currentValue = Double.parseDouble(itr.nextToken());

          coordinates.add(new DoubleWritable(currentValue));
          counter++;
      }

      point.setCoordinates(coordinates);

      Centroid closestCentroid = null;
      Double minDistance = Double.MAX_VALUE;
      Double distance;

      for (Centroid currentCentroid : centroids) {
          distance = currentCentroid.findEuclideanDistance(point);

          if (minDistance > distance) {
              closestCentroid = currentCentroid;
              minDistance = distance;
          }
      }

      context.write(closestCentroid, point);
    }
  }

  public static class KMeansReducer extends Reducer<Centroid, Point, Text, Text> {
    public static enum Counter {
      CONVERGED
    }
    private Text key = new Text("");
    private Text value = new Text("");
    private static int DIMENSION;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      DIMENSION = Integer.parseInt(conf.get("dimension"));
    }

    @Override
    public void reduce(Centroid centroid, Iterable<Point> values, Context context) throws IOException, InterruptedException {
      String results = "";

      Centroid auxiliarCenter = new Centroid(DIMENSION);

      for (Point point : values) {
        auxiliarCenter.add(point);

        key.set(centroid.toString());
        value.set(auxiliarCenter.toString());

        context.write(key, value);
      }


      System.out.println("CENTER::" + centroid);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 6) {
      System.err.println("Usage: kmeans <input> <k> <dimension> <threshold> <centroidsFilename> <output>");
      System.exit(2);
    }

    System.out.println("args[0]: <input>=" + otherArgs[0]);
    System.out.println("args[1]: <k>=" + otherArgs[1]);
    System.out.println("args[2]: <dimension>=" + otherArgs[2]);
    System.out.println("args[3]: <threshold>=" + otherArgs[3]);
    System.out.println("args[4]: <centroidsFilename>=" + otherArgs[4]);
    System.out.println("args[5]: <output>=" + otherArgs[5]);

    // createcentroids(Integer.parseInt(otherArgs[1]), conf, new Path(otherArgs[4]));

    Job job = Job.getInstance(conf, "kmean");
    job.getConfiguration().set("k", otherArgs[1]);
    job.getConfiguration().set("dimension", otherArgs[2]);
    job.getConfiguration().set("threshold", otherArgs[3]);
    job.getConfiguration().set("centroidsFilename", otherArgs[4]);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setJarByClass(Kmeans.class);
    job.setMapperClass(KMeansMapper.class);
    job.setReducerClass(KMeansReducer.class);

    job.setMapOutputKeyClass(Centroid.class);
    job.setMapOutputValueClass(Point.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  private static void createcentroids(int k, Configuration conf, Path centroids) throws IOException {
    SequenceFile.Writer centroidWriter = SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(centroids),
            SequenceFile.Writer.keyClass(IntWritable.class),
            SequenceFile.Writer.valueClass(Centroid.class));

    List<DoubleWritable> listParameters = new ArrayList<DoubleWritable>();
    Centroid auxiliarCentroid;

    double[][] arrays = {
      {0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916,0.0625,0.670639},
      {0.297959,0.458886,0.220056,0.074303,0.247911,0.072423,0.172702,0.0625,0.703142},
      {0.297959,0.453581,0.239554,0.06192,0.256267,0.064067,0.208914,0.05625,0.698808}
    };

    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
          listParameters.add(new DoubleWritable(arrays[i][j]));
        }

        auxiliarCentroid = new Centroid(new IntWritable(i), listParameters);
        centroidWriter.append(new IntWritable(i), auxiliarCentroid);
        listParameters = new ArrayList<DoubleWritable>();
    }

    centroidWriter.close();
  }
}
