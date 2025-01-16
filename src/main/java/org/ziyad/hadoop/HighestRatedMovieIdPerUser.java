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

public class HighestRatedMovieIdPerUser {

    static Logger log = Logger.getLogger(HighestRatedMovieTitlePerUser.class.getName());

    // mapper: outputs (user id , movie id + rating)
    public static class UserRatingsMapper extends Mapper<LongWritable, Text, LongWritable, MovieIdRatingWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // input: ratings.csv

            // skip first line containing column names
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();
            String[] fields = line.split(","); //userId,movieId,rating

            long userId = Long.parseLong(fields[0]);
            long movieId = Long.parseLong(fields[1]);
            double rating = Double.parseDouble(fields[2]);

            MovieIdRatingWritable outputVal = new MovieIdRatingWritable();
            outputVal.setMovieId(movieId);
            outputVal.setRating(rating);

            context.write(new LongWritable(userId), outputVal);
        }
    }

    // reducer: outputs (user id , highest rated movie id)
    public static class HighestIDReducer extends Reducer<LongWritable, MovieIdRatingWritable, LongWritable, LongWritable> {

        public void reduce(LongWritable userId, Iterable<MovieIdRatingWritable> movieIdRatingList, Context context) throws IOException, InterruptedException {

            long highestRatedId = 0;
            double highestRating = 0.0;

            for (MovieIdRatingWritable movieIdRating : movieIdRatingList) {

                double rating = movieIdRating.getRating();

                if (rating > highestRating) {
                    long movieId = movieIdRating.getMovieId();
                    highestRating = rating;
                    highestRatedId = movieId;
                }
            }

            context.write(userId, new LongWritable(highestRatedId));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HighestRatedMovieIdPerUser <ratings.csv_input_path>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "find highest rated movie id per user");
        job.setJarByClass(HighestRatedMovieIdPerUser.class);

        // mapper
        job.setMapperClass(HighestRatedMovieIdPerUser.UserRatingsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // user id
        job.setMapOutputValueClass(MovieIdRatingWritable.class); // movie id and rating

        // reducer
        job.setReducerClass(HighestRatedMovieIdPerUser.HighestIDReducer.class);
        job.setOutputKeyClass(LongWritable.class); // user id
        job.setOutputValueClass(LongWritable.class); // movie id

        // input and output paths
        TextInputFormat.addInputPath(job, new Path(args[0])); // ratings.csv
        FileOutputFormat.setOutputPath(job, new Path("/output1")); // output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // the output for this question will be in /output1/part-r-00000
    }
}
