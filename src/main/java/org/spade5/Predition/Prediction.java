package org.spade5.Predition;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.logging.Logger;

public class Prediction {

    private static Hashtable<String,Double> class_prob = new Hashtable<String, Double>();
    private static Hashtable<Map<String,String>,Double> class_term_prob = new Hashtable<Map<String, String>, Double>();
    private static Hashtable<String,Double> class_term_total = new Hashtable<String, Double>();
    private static Hashtable<String,Double> class_term_num = new Hashtable<String, Double>();

    public static final String HDFS_ROOT_URL="hdfs://hadoop0:9000";
    private Configuration conf;
    private static final Logger log = Logger.getLogger(Prediction.class.getName());
    Prediction() throws Exception {
        // 统计文档总数
        conf = new Configuration();
        log.info("Prediction initializing...");
        BufferedReader reader = readFile(HDFS_ROOT_URL+ "/output_doc/" + "part-r-00000");
        double file_total = 0;
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");
            file_total += Double.valueOf(args[1]);
        }

        // 计算先验概率class_prob
        reader = readFile(HDFS_ROOT_URL+ "/output_doc/" + "part-r-00000");
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");
            class_prob.put(args[0],Double.valueOf(args[1])/file_total);
        }

        //计算单词总数
        reader = readFile(HDFS_ROOT_URL+ "/output_word/" + "part-r-00000");
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.valueOf(args[2]);
            String classname = args[0];
            class_term_total.put(classname, class_term_total.getOrDefault(classname,0.0) + count);
        }

        //计算单词集合大小
        reader = readFile(HDFS_ROOT_URL+ "/output_word/" + "part-r-00000");
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            String classname = args[0];
            class_term_num.put(classname, class_term_num.getOrDefault(args[0],0.0) + 1.0);
        }

        //计算每个类别里面出现的词条概率class-term prob
        reader = readFile(HDFS_ROOT_URL+ "/output_word/" + "part-r-00000");
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.valueOf(args[2]);
            String classname = args[0];
            String term = args[1];
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname,term);
            class_term_prob.put(map, (count+1)/(class_term_total.get(classname) + class_term_num.get(classname)));
        }

        System.out.println(class_prob);
        System.out.println(class_term_total);
        System.out.println(class_term_num);
        System.out.println(class_term_prob);
    }

    public static double conditionalProbabilityForClass(String content, String classname){
        // 计算一个文档属于某类的条件概率
        double result = 0;
        log.info("calc " + content + " in " + classname);
        String[] words = content.split("\n");
        for(String word:words){
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname, word);
            result += Math.log(
                    class_term_prob.getOrDefault(map,
                            1.0/(
                                    class_term_total.get(classname) +
                                    class_term_num.get(classname)
                            )
                    )
            );
        }
        result += Math.abs(Math.log(class_prob.get(classname)));
        return result;
    }

    private BufferedReader readFile(String uri) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
        return reader;
    }
}