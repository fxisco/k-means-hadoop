package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
    private static int DIMENSION;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsFilename"));
        DIMENSION = Integer.parseInt(conf.get("dimension"));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroidsPath));
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();

        while (reader.next(key, value)) {
          Centroid c = new Centroid(key, value.getCoordinates());

          c.setId(key);
          centroids.add(c);
        }

        reader.close();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
              closestCentroid = Centroid.copy(currentCentroid);
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

    Text key = new Text("");
    Text value = new Text("");
    static int DIMENSION;
    static Double THRESHOLD;
    private final List<Centroid> centroidsList = new ArrayList<>();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      DIMENSION = Integer.parseInt(conf.get("dimension"));
      THRESHOLD = Double.parseDouble(conf.get("threshold"));
    }

    @Override
    public void reduce(Centroid centroid, Iterable<Point> values, Context context) throws IOException, InterruptedException {
      Centroid auxiliarCentroid = new Centroid(DIMENSION);

      int numElements = 0;

      for (Point point : values) {
        auxiliarCentroid.add(point);

        key.set(centroid.toString());
        value.set(point.toString());
        numElements++;

        context.write(key, value);
      }

      auxiliarCentroid.setId(centroid.getId());
      auxiliarCentroid.mean(numElements);

      Centroid copy = Centroid.copy(auxiliarCentroid);

      centroidsList.add(copy);

      if (centroid.isConverging(auxiliarCentroid, THRESHOLD)) {
        context.getCounter(Counter.CONVERGED).increment(1);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Configuration conf = context.getConfiguration();
      Path outPath = new Path(conf.get("centroidsFilename"));
      FileSystem fs = FileSystem.get(conf);

      fs.delete(outPath, true);

      SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(outPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Centroid.class));

      int i = 0;

      for (Centroid centroid : centroidsList) {
        writer.append(new IntWritable(i), centroid);
        i++;
      }

      writer.close();
    }
  }

  public static class RandomCentroidsMapper extends Mapper<Object, Text, IntWritable, Text> {
    private final IntWritable randomKey = new IntWritable();
    private final Text point = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      int random = (int) Math.round(Math.random());
      randomKey.set(random);
      point.set(value.toString());

      context.write(randomKey, point);
    }
  }

  public static class RandomCentroidsReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    static int DIMENSION;
    static int K;
    static List<DoubleWritable> coordinates;
    private final List<Centroid> centroidsList = new ArrayList<>();
    static int counter = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();

      DIMENSION = Integer.parseInt(conf.get("dimension"));
      K = Integer.parseInt(conf.get("k"));
    }

    @Override
    public void reduce(IntWritable randomInteger, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text point : values) {
        int coordinatesCounter = 0;

        if (counter < K) {
          coordinates = new ArrayList<DoubleWritable>();
          StringTokenizer itr = new StringTokenizer(point.toString(), ",");

          while (itr.hasMoreTokens()) {
            if (coordinatesCounter == DIMENSION)
              break;

            double currentValue = Double.parseDouble(itr.nextToken());

            coordinates.add(new DoubleWritable(currentValue));
            coordinatesCounter++;
          }

          Centroid centroid = new Centroid(new IntWritable(counter), coordinates);
          centroidsList.add(centroid);
          counter++;

          context.write(null, point);
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Configuration conf = context.getConfiguration();
      Path outPath = new Path(conf.get("centroidsFilename"));
      FileSystem fs = FileSystem.get(conf);

      if (fs.exists(outPath)) {
        System.out.println("Delete old output folder: " + outPath.toString());
        fs.delete(outPath, true);
      }

      SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(outPath),
                SequenceFile.Writer.keyClass(IntWritable.class),
                SequenceFile.Writer.valueClass(Centroid.class));

      int i = 0;

      for (Centroid centroid : centroidsList) {
        writer.append(new IntWritable(i), centroid);
        i++;
      }

      writer.close();
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

    Job centroidsJob = Job.getInstance(conf, "centroids");

    centroidsJob.getConfiguration().set("k", otherArgs[1]);
    centroidsJob.getConfiguration().set("dimension", otherArgs[2]);
    centroidsJob.getConfiguration().set("centroidsFilename", otherArgs[4]);

    centroidsJob.setJarByClass(Kmeans.class);
    centroidsJob.setMapperClass(RandomCentroidsMapper.class);
    centroidsJob.setReducerClass(RandomCentroidsReducer.class);
    centroidsJob.setMapOutputKeyClass(IntWritable.class);
    centroidsJob.setMapOutputValueClass(Text.class);
    centroidsJob.setOutputKeyClass(Text.class);
    centroidsJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(centroidsJob, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(centroidsJob, new Path(otherArgs[5]));

    centroidsJob.setInputFormatClass(TextInputFormat.class);
    centroidsJob.setOutputFormatClass(TextOutputFormat.class);

    centroidsJob.waitForCompletion(true);

    // createCentroids(conf, new Path(otherArgs[4]));
    System.out.println("::INITIAL CENTROIDS::");
    readCentroids(conf, new Path(otherArgs[4]));

    Job kmeansJob;
    Path output = new Path(otherArgs[5]);
    FileSystem fs = FileSystem.get(output.toUri(),conf);
    long convergedCentroids = 0;
    int k = Integer.parseInt(args[2]);
    int iterations = 0;

    while (convergedCentroids < k) {
      if (fs.exists(output)) {
        System.out.println("Delete old output folder: " + output.toString());
        fs.delete(output, true);
      }

      kmeansJob = Job.getInstance(conf, "kmean");

      kmeansJob.getConfiguration().set("k", otherArgs[1]);
      kmeansJob.getConfiguration().set("dimension", otherArgs[2]);
      kmeansJob.getConfiguration().set("threshold", otherArgs[3]);
      kmeansJob.getConfiguration().set("centroidsFilename", otherArgs[4]);

      kmeansJob.setJarByClass(Kmeans.class);
      kmeansJob.setMapperClass(KMeansMapper.class);
      kmeansJob.setReducerClass(KMeansReducer.class);
      kmeansJob.setMapOutputKeyClass(Centroid.class);
      kmeansJob.setMapOutputValueClass(Point.class);
      kmeansJob.setOutputKeyClass(Text.class);
      kmeansJob.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(kmeansJob, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(kmeansJob, new Path(otherArgs[5]));

      kmeansJob.setInputFormatClass(TextInputFormat.class);
      kmeansJob.setOutputFormatClass(TextOutputFormat.class);

      kmeansJob.waitForCompletion(true);

      convergedCentroids = kmeansJob.getCounters().findCounter(KMeansReducer.Counter.CONVERGED).getValue();

      iterations++;
    }

    System.out.println("Number of iterations\t" + iterations);
    System.out.println("::FINAL CENTROIDS::");
    readCentroids(conf, new Path(otherArgs[4]));
  }

  private static void createCentroids(Configuration conf, Path centroids) throws IOException {
    SequenceFile.Writer centroidWriter = SequenceFile.createWriter(conf,
            SequenceFile.Writer.file(centroids),
            SequenceFile.Writer.keyClass(IntWritable.class),
            SequenceFile.Writer.valueClass(Centroid.class));

    List<DoubleWritable> listParameters = new ArrayList<DoubleWritable>();
    Centroid auxiliarCentroid;

    double[][] arrays = {
      {0.297959,0.469496,0.211699,0.077399,0.256267,0.08078,0.169916,0.0625,0.670639},
      {0.297959,0.469496,0.220056,0.074303,0.247911,0.072423,0.172702,0.0625,0.703142},
      {0.297959,0.469496,0.239554,0.06192,0.256267,0.064067,0.208914,0.05625,0.698808},
      {0.297959,0.469496,0.239554,0.06192,0.256267,0.064067,0.208914,0.05625,0.698808}
    };

    for (int i = 0; i < arrays.length; i++) {
        for (int j = 0; j < 2; j++) {
          listParameters.add(new DoubleWritable(arrays[i][j]));
        }

        auxiliarCentroid = new Centroid(new IntWritable(i), listParameters);
        centroidWriter.append(new IntWritable(i), auxiliarCentroid);
        listParameters = new ArrayList<DoubleWritable>();
    }

    centroidWriter.close();
  }

  private static void readCentroids(Configuration conf, Path centroids) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(centroids));
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();

        while (reader.next(key, value)) {
          // Centroid c = new Centroid(key, value.getCoordinates());
          System.out.println(value);
        }

        reader.close();
  }
}
