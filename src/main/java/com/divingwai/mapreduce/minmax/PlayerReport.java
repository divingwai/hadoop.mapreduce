package com.divingwai.mapreduce.minmax;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlayerReport implements Writable {
    private Text playerName = new Text();
    private IntWritable maxScore= new IntWritable();
    private Text maxScoreopposition= new Text();
    private IntWritable minScore= new IntWritable();
    private Text minScoreopposition= new Text();

    public void write(DataOutput dataOutput) throws IOException {
        playerName.write(dataOutput);
        maxScore.write(dataOutput);
        maxScoreopposition.write(dataOutput);
        minScore.write(dataOutput);
        minScoreopposition.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        playerName.readFields(dataInput);
        maxScore.readFields(dataInput);
        maxScoreopposition.readFields(dataInput);
        minScore.readFields(dataInput);
        minScoreopposition.readFields(dataInput);
    }

    public Text getPlayerName() {
        return playerName;
    }

    public void setPlayerName(Text playerName) {
        this.playerName = playerName;
    }

    public IntWritable getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(IntWritable maxScore) {
        this.maxScore = maxScore;
    }

    public Text getMaxScoreopposition() {
        return maxScoreopposition;
    }

    public void setMaxScoreopposition(Text maxScoreopposition) {
        this.maxScoreopposition = maxScoreopposition;
    }

    public IntWritable getMinScore() {
        return minScore;
    }

    public void setMinScore(IntWritable minScore) {
        this.minScore = minScore;
    }

    public Text getMinScoreopposition() {
        return minScoreopposition;
    }

    public void setMinScoreopposition(Text minScoreopposition) {
        this.minScoreopposition = minScoreopposition;
    }

    @Override
    public String toString() {
        return playerName +
                "\t" + maxScore +
                "\t" + maxScoreopposition +
                "\t" + minScore +
                "\t" + minScoreopposition;
    }
}