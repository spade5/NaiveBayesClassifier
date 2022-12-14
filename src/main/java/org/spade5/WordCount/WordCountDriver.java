package org.spade5.WordCount;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {"/input/train", "/output_word"};

        // 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        FileSystem fs = FileSystem.get(configuration);

        // 2 获取jar包位置
        job.setJarByClass(WordCountDriver.class);

        // 3 关联自定义的mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出数据类型
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出数据类型
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出文件路径
        ArrayList<Path> paths = GetPaths(fs, new Path(args[0]));
        for(int i=0; i < paths.size(); i++) {
            FileInputFormat.addInputPath(job, paths.get(i));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交代码
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

    private static ArrayList<Path> GetPaths(FileSystem fs, Path path) throws IOException {
        // 获取path路径下所有子文件夹路径
        ArrayList<Path> paths = new ArrayList<Path>();

        if (fs.exists(path)) {
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                Path onePath = fileStatus.getPath();
                if (fileStatus.isDirectory()) {
                    paths.addAll(GetPaths(fs, onePath));
                } else if (onePath.getName().contains(".txt")) {
                    paths.add(onePath);
                }
            }
        }

        return paths;
    }
}
