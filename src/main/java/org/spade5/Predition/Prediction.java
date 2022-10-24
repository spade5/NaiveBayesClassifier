package org.spade5.Predition;

import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InputStreamReader;
import org.apache.hadoop.fs.Path;

public class Prediction {

    private static Hashtable<String,Double> class_prob = new Hashtable<String, Double>();
    private static Hashtable<Map<String,String>,Double> class_term_prob = new Hashtable<Map<String, String>, Double>();
    private static Hashtable<String,Double> class_term_total = new Hashtable<String, Double>();
    private static Hashtable<String,Double> class_term_num = new Hashtable<String, Double>();

    public static final String HDFS_ROOT_URL="hdfs://localhost:9000";
    private Configuration conf;
    Prediction() throws NumberFormatException, IOException{
        // 统计文档总数
        BufferedReader reader = readFile(HDFS_ROOT_URL+ "/output_doc/" + "part-r-00000");
        double file_total = 0;
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");
            file_total += Double.valueOf(args[1]);
            // 计算先验概率class_prob
            class_prob.put(args[0],Double.valueOf(args[1])/file_total);
            System.out.println(String.format(("%s:%f"),args[0],Double.valueOf(args[1])/file_total));
        }

        //计算单词总数
        reader = readFile(HDFS_ROOT_URL+ "/output_word/" + "part-r-00000");
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.valueOf(args[2]);
            String classname = args[0];
            class_term_total.put(classname,class_term_total.getOrDefault(classname,0.0)+count);

            //计算单词集合大小
            class_term_num.put(classname,class_term_num.getOrDefault(args[0],0.0)+1.0);

            //计算每个类别里面出现的词条概率class-term prob
            double count = Double.valueOf(args[2]);
            String term = args[1];
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname,term);
            class_term_prob.put(map, (count+1)/(class_term_total.get(classname)+class_term_num.get(classname)));
        }
    }

    public static double conditionalProbabilityForClass(String content,String classname){
        // 计算一个文档属于某类的条件概率
        double result = 0;
        String[] words = content.split("\n");
        for(String word:words){
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname, word);
            result += Math.log(class_term_prob.getOrDefault(map,1.0/(class_term_total.get(classname)+class_term_num.get(classname))));
        }
        result += Math.abs(Math.log(class_prob.get(classname)));
        return result;
    }

    private BufferedReader readFile(String uri) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        BufferedReader reader
        try {
            reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
        } finally {
            IOUtils.closeStream(in);
        }
        return reader;
    }
}