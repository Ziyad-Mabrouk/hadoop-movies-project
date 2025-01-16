package org.ziyad.hadoop;

import java.util.logging.Logger;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class HighestRatedMovieTitlePerUser {

    static Logger log = Logger.getLogger(HighestRatedMovieTitlePerUser.class.getName());

    // run HighestRatedMovieIdPerUser as first job

    // for job 2: we have two mappers
    // mapper 1: outputs (movie id , $ movie title)
    public static class MovieTitleMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: movies.csv

            // skip first line containing column names
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();

            // we use this regex to avoid comma problems when splitting for movies with comma in their titles
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // movieId,title,genres

            long movieId = Long.parseLong(fields[0]);
            String movieTitle = "$" + fields[1]; // we use $ to distinguish between the output of mapper 1 and mapper 2

            context.write(new LongWritable(movieId), new Text(movieTitle));
        }
    }

    // mapper 2: outputs (highest rated movie id , user id)
    public static class UserMovieSwapMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: output of job 1

            String line = value.toString();
            String[] fields = line.split("\t"); // userId   highest rated movie id

            String userId = fields[0];
            long movieId = Long.parseLong(fields[1]);

            // we use LongWritable for the userId to match the Text output value of mapper 1 in the reducer
            context.write(new LongWritable(movieId), new Text(userId));
        }
    }

    // reducer: outputs (user id , highest rated movie title)
    public static class JoinReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        public void reduce(LongWritable movieId, Iterable<Text> valuesToJoin, Context context) throws IOException, InterruptedException {

            String movieTitle = "";

            for (Text val : valuesToJoin) {
                String title = val.toString();

                if (title.startsWith("$")){
                    // it's a title comping from mapper 1
                    movieTitle = title.replaceFirst("^\\$", "");
                    break;
                }
            }

            for (Text val : valuesToJoin) {
                String userId = val.toString();

                if (!userId.startsWith("$")){
                    // it's a user Id coming from mapper 2
                    context.write(new LongWritable(Long.parseLong(userId)), new Text(movieTitle));
                }
            }
        }

    }

    // for job 3: we sort output by user id

    // mapper:
    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: output of job 2

            String line = value.toString();
            String[] fields = line.split("\t"); // userId   highest rated movie name

            long userId = Long.parseLong(fields[0]);
            String movieTitle = fields[1];

            context.write(new LongWritable(userId), new Text(movieTitle));
        }
    }

    // reducer: outputs sorted (user id , highest rated movie title)
    public static class SortReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        public void reduce(LongWritable userId, Iterable<Text> movieTitle, Context context) throws IOException, InterruptedException {

            Text title = new Text();

            // normally, the iterable should only have one value because for each user we output only one highest rated movie
            for (Text unique_val : movieTitle) {
                title = unique_val;
            }

            context.write(userId, title);
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HighestRatedMovieTitlePerUser <movies.csv_input_path>");
            System.exit(-1);
        }

        // job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "join movie titles with user's highest rated movie");
        job1.setJarByClass(HighestRatedMovieTitlePerUser.class);

        // multiple inputs for job 1
        MultipleInputs.addInputPath(job1, new Path("/output1/part-r-00000"), TextInputFormat.class, UserMovieSwapMapper.class); // Output from HighestRatedMovieIdPerUser
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MovieTitleMapper.class); // movies.csv

        // reducer for job 1
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(LongWritable.class); // user id
        job1.setOutputValueClass(Text.class); // movie title

        // output path for Job 1
        FileOutputFormat.setOutputPath(job1, new Path("/output2")); // output path for unsorted

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed, exiting.");
            System.exit(1);
        }

        // job 2 : sorting
        Configuration conf2 = new Configuration();
        Job job = Job.getInstance(conf2, "find highest rated movie title per user sorted by user");
        job.setJarByClass(HighestRatedMovieTitlePerUser.class);

        // mapper for job 2
        job.setMapperClass(HighestRatedMovieTitlePerUser.SortMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // user id
        job.setMapOutputValueClass(Text.class); // movie title

        // reducer for job 2
        job.setReducerClass(HighestRatedMovieTitlePerUser.SortReducer.class);
        job.setOutputKeyClass(LongWritable.class); // user id
        job.setOutputValueClass(Text.class); // movie title

        // input and output paths for job 2
        TextInputFormat.addInputPath(job, new Path("/output2/part-r-00000")); // output of job 2
        FileOutputFormat.setOutputPath(job, new Path("/output3")); // final output path for sorted

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // the unsorted output for this question will be in /output2/part-r-00000
    // the sorted output will be in /output3/part-r-00000
}
