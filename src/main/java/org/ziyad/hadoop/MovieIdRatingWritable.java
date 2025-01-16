package org.ziyad.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieIdRatingWritable implements Writable {
    private long movieId;
    private double rating;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(movieId);
        out.writeDouble(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieId = in.readLong();
        rating = in.readDouble();
    }

    @Override
    public String toString() {
        return "IDRatingWritable{" +
                "movieId='" + movieId + '\'' +
                ", rating=" + rating + '\'' +
                '}';
    }

    public long getMovieId() {
        return movieId;
    }

    public void setMovieId(long movieId) {
        this.movieId = movieId;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }
}
