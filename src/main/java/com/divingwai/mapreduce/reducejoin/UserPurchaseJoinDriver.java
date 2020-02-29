package com.divingwai.mapreduce.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class UserPurchaseJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), (Tool) new UserPurchaseJoinDriver(),
                args);
        System.exit(res);
    }

    
    public int run(String[] args) throws Exception {
   
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "UserPurchaseJoin");
        job.setJarByClass(UserPurchaseJoinDriver.class);
        if (args.length < 3) {
            System.out.println("Jar requires 3 paramaters : \""
                    + job.getJar()
                    + " input_user_path input_purchase_path output_path");
            return 1;
        }
        
        //job.setMapperClass(UserMapper.class);
        
//        job.setMapperClass(PurchaseReportMapper.class);
       job.setReducerClass(UserPurchaseJoinReducer.class);
//        
        //output format for mapper
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

        //output format for reducer
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //setting input / output
        Path inputUserPath = new Path(args[0]);
        Path inputPurchasePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        
        System.out.printf("input user path: %s", inputUserPath);
        System.out.printf("input purchase path: %s", inputPurchasePath);

        	MultipleInputs.addInputPath(job, inputUserPath,  TextInputFormat.class, UserMapper.class);
	MultipleInputs.addInputPath(job, inputPurchasePath, TextInputFormat.class, PurchaseReportMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        
        job.waitForCompletion(true);
        return 0;
    }
}