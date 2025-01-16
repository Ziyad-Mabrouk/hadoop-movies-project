package org.ziyad.hadoop;

import java.util.ArrayList;
import java.util.logging.Logger;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieTitleListByLikedCount {

    static Logger log = Logger.getLogger(HighestRatedMovieTitlePerUser.class.getName());

    // for job 1-3: run HighestRatedMovieTitlePerUser to get sorted (user id , highest rated movie title)

    // for job 4:
    // mapper:
    public static class UserTitleSwapMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: output of job 3

            String line = value.toString();
            String[] fields = line.split("\t"); // userId   highest rated movie title

            long userId = Long.parseLong(fields[0]);
            String title = fields[1];

            context.write(new Text(title), new LongWritable(userId));
        }
    }

    // reducer: outputs (movie title , count)
    public static class UserCountReducer extends Reducer<Text, LongWritable, Text, IntWritable> {

        public void reduce(Text movieTitle, Iterable<LongWritable> userIds, Context context) throws IOException, InterruptedException{

            int count = 0;

            for (LongWritable userId : userIds) {
                count ++;
            }

            context.write(movieTitle, new IntWritable(count));
        }
    }


    // for job 5:
    //  mapper: outputs (count , movie title)
    public static class TitleCountSwapperMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: output of job 4

            String line = value.toString();
            String[] fields = line.split("\t"); // movie title   count

            String movieTitle = fields[0];
            int count = Integer.parseInt(fields[1]);

            context.write(new IntWritable(count), new Text(movieTitle));
        }
    }

    // reducer: outputs (count , list of movie titles)
    public static class TitleListReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable count, Iterable<Text> movieTitles, Context context) throws IOException, InterruptedException{

            ArrayList<String> titleList = new ArrayList<>();

            for (Text movieTitle : movieTitles) {
                titleList.add(movieTitle.toString());
            }

            String stringTitleList = String.join(" | ", titleList);

            context.write(count, new Text(stringTitleList));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 0) {
            System.err.println("Usage: MovieTitleListByLikedCount");
            System.exit(-1);
        }

        // job 1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "count users per movie title");
        job1.setJarByClass(MovieTitleListByLikedCount.class);

        // mapper and Reducer for job 1
        job1.setMapperClass(MovieTitleListByLikedCount.UserTitleSwapMapper.class);
        job1.setReducerClass(MovieTitleListByLikedCount.UserCountReducer.class);

        // mapper output key and value types
        job1.setMapOutputKeyClass(Text.class); // movie title
        job1.setMapOutputValueClass(LongWritable.class); // user id

        // reducer output key and value types
        job1.setOutputKeyClass(Text.class); // movie title
        job1.setOutputValueClass(IntWritable.class); // count

        // input and output paths for job 1
        FileInputFormat.addInputPath(job1, new Path("/output3/part-r-00000")); // input: sorted output of HighestRatedMovieTitlePerUser
        FileOutputFormat.setOutputPath(job1, new Path("/output4")); // temp output for job 1

        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed, exiting.");
            System.exit(1);
        }

        // job 2:
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "generate movie title list by their liked count");
        job2.setJarByClass(MovieTitleListByLikedCount.class);

        // mapper and reducer for job 2
        job2.setMapperClass(MovieTitleListByLikedCount.TitleCountSwapperMapper.class);
        job2.setReducerClass(MovieTitleListByLikedCount.TitleListReducer.class);

        // mapper output key and value types
        job2.setMapOutputKeyClass(IntWritable.class); // count
        job2.setMapOutputValueClass(Text.class); // movie title

        // reducer output key and value types
        job2.setOutputKeyClass(IntWritable.class); // count
        job2.setOutputValueClass(Text.class); // list of movie titles

        // input and output paths for job 2
        FileInputFormat.addInputPath(job2, new Path("/output4/part-r-00000")); // input: output of job 2
        FileOutputFormat.setOutputPath(job2, new Path("/output5")); // final output path

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

        // the sorted output for this question will be in /output5/part-r-00000
    }

}
