package com.divingwai.mapreduce.wordcount;

import com.jcraft.jsch.ConfigRepository;
import com.jcraft.jsch.JSch;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoteDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        
        //set the configuration here 
        Configuration conf = new Configuration();
        
        //set hdfs address
        conf.set("fs.defaultFS", "hdfs://hdubuscvm01:9000/"); 
        //set yarn address
        conf.set("yarn.resourcemanager.address", "hdubuscvm01:8032"); 
        // set mapreduce framework
        conf.set("mapreduce.framework.name", "yarn"); 
        
        //set jar path
      // conf.set("mapreduce.job.jar","./target/mapreduce-1.0-SNAPSHOT.jar");
        
        
       // conf.set("hadoop.job.user", "patrick");
        
         conf.set("yarn.application.classpath",  
         "$HADOOP_HOME/etc/hadoop,"  //$HADOOP_CONF_DIR
        + "$HADOOP_HOME/share/hadoop/common/lib/*," // $HADOOP_COMMON_HOME/lib/*,
            + "$HADOOP_HOME/share/hadoop/common/*," // $HADOOP_COMMON_HOME/*
                 + "$HADOOP_HOME/share/hadoop/hdfs," 
            +  "$HADOOP_HOME/share/hadoop/hdfs/lib/*," //$HADOOP_HDFS_HOME/lib/*,"
                 + "$HADOOP_HOME/share/hadoop/hdfs/*," //"$HADOOP_HDFS_HOME/*,
            +"$HADOOP_HOME/share/hadoop/mapreduce/lib/*," //$HADOOP_MAPRED_HOME/lib/*
                 + "$HADOOP_HOME/share/hadoop/mapreduce/*," //$HADOOP_MAPRED_HOME/*
            + "$HADOOP_HOME/share/hadoop/yarn,"             
                 + "$HADOOP_HOME/share/hadoop/yarn/lib/*,"  //$HADOOP_YARN_HOME/lib/*
                 + "$HADOOP_HOME/share/hadoop/yarn/*"); // "$HADOOP_YARN_HOME/*



        
        int res = ToolRunner.run(conf, (Tool) new RemoteDriver(),
                args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception {
   
        Configuration conf = getConf();
        
//        System.out.print(conf.get("yarn.resourcemanager.address")); 
//        System.out.print(conf.get("mapreduce.framework.name")); 
//        System.out.print(conf.get("fs.defaultFS")); 
//        String input=conf.get("input");
//        String output=conf.get("output");
//        FileSystem inFs=
//            FileSystem.get(
//                URI.create(input),conf);
//        FSDataInputStream is=
//            inFs.open(new Path(input));
//        FileSystem outFs=
//            FileSystem.getLocal(conf);
//        FSDataOutputStream os=
//            outFs.create(new Path(output));
//        IOUtils.copyBytes(is,os,conf,true);
//        
//// 获取从集群上读取文件的文件系统对象
//        // 和输入流对象
//        FileSystem inFs = FileSystem.get(
//                URI.create("file://1.0.0.5:9000/user/kevin/passwd"),conf);
//        FSDataInputStream is= inFs.open(new Path("hdfs://1.0.0.5:9000/user/kevin/passwd"));
//        // 获取本地文件系统对象
//        LocalFileSystem outFs = FileSystem.getLocal(conf);
//        FSDataOutputStream os = outFs.create(new Path("C:\\passwd"));
//        byte[] buff=new byte[1024];
//        int length=0;
//        while((length=is.read(buff))!=-1){
//            os.write(buff,0,length);
//            os.flush();
//        }
//        System.out.println(
//            inFs.getClass().getName());
//        System.out.println(
//            is.getClass().getName());
//        System.out.println(
//            outFs.getClass().getName());
//        System.out.println(
//            os.getClass().getName());
//        os.close();
//        is.close();
//        return 0;
//        
        
        
        
        
        
        Job job = Job.getInstance(conf, "WordCount");
        
        job.setJar("./target/mapreduce-1.0-SNAPSHOT.jar");
        job.setJarByClass(Driver.class);
        
        if (args.length == 0)
        {
            args = new String[] {
             //   "hdfs://hdubuscvm01:9000/hadoop/mapreduce/wordcount/input",
                
           //    "hdfs://hdubuscvm01:9000/hadoop/mapreduce/wordcount/output",
              //  "/Users/patrick/VSCProjects/hadoop/mapreduce/wordcount/input",
               // "/Users/patrick/VSCProjects/hadoop/mapreduce/wordcount/output"
                   "/hadoop/mapreduce/wordcount/input",
                
               "/hadoop/mapreduce/wordcount/output",
            };
            
        }
        
        if (args.length < 2) {
            System.out.println("Jar requires 2 paramaters : \""
                    + job.getJar()
                    + " input_path output_path");
            return 1;
        }
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);
        job.setCombinerClass(WordcountReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, filePath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        return 0;
    }
}