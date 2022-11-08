package org.spade5.DocCount;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocCountDriver {

    public static void main(String[] _args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        String[] args;
        if (_args == null || _args.length == 0) {
            args = new String[] {"/input/train", "/output_doc"};
        } else {
            args = _args;
        }

        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        FileSystem fs = FileSystem.get(conf);

        // 2 获取jar包位置
        job.setJarByClass(DocCountDriver.class);

        // 3 关联自定义的mapper和reducer
        job.setMapperClass(DocCountMapper.class);
        job.setReducerClass(DocCountReducer.class);

        // 4 设置自定义的InputFormat类
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 5 设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 6 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 7 设置输入和输出文件路径
        ArrayList<Path> paths = GetPaths(fs, new Path(args[0]));

        for(int i=0; i < paths.size(); i++) {
            FileInputFormat.addInputPath(job, paths.get(i));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 提交代码
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