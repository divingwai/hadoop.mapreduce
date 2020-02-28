package com.divingwai.mapreduce.movierating;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {
    private int K = 3;
    private TreeMap<String, String> topMoviesByRating = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        System.out.println("in reducer reduce ");
        for (Text movie : values) {
             System.out.print(key.toString() + ": " + movie.toString());
            topMoviesByRating.put(key.toString(), movie.toString());
            if (topMoviesByRating.size() > K) {
                topMoviesByRating.remove(topMoviesByRating.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        System.out.println("in reducer cleanup");
        for (Map.Entry<String, String> movieDetail :
                topMoviesByRating.entrySet()) {
            context.write(new Text(movieDetail.getKey()), new
                    Text(movieDetail.getValue()));
        }
    }


}
