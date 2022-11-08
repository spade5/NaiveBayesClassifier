package org.spade5.Predition;

import java.io.*;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Prediction {

    private static Hashtable<String,Double> class_prob = new Hashtable<String, Double>();
    private static Hashtable<Map<String,String>,Double> class_term_prob = new Hashtable<Map<String, String>, Double>();
    private static Hashtable<String,Double> class_term_total = new Hashtable<String, Double>();
    private static Hashtable<String,Double> class_term_num = new Hashtable<String, Double>();
    private static String DocPathStr = "/output_doc/part-r-00000";
    private static String WordPathStr = "/output_word/part-r-00000";

    private Configuration conf;
    Prediction() throws Exception {
        // 统计文档总数
        conf = new Configuration();
//        Utils.outInfo("Prediction initializing...");

        // 统计文档总数
        BufferedReader reader = readFile(DocPathStr);
        double file_total = 0;
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");
            file_total += Double.valueOf(args[1]);
        }
        // 计算先验概率class_prob
        reader = readFile(DocPathStr);
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");
            class_prob.put(args[0],Double.valueOf(args[1])/file_total);
            System.out.println(String.format(("%s:%f"),args[0],Double.valueOf(args[1])/file_total));
        }

        //计算单词总数
        reader = readFile(WordPathStr);
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.valueOf(args[2]);
            String classname = args[0];
            class_term_total.put(classname,class_term_total.getOrDefault(classname,0.0)+count);
        }
        //计算单词集合大小
        reader = readFile(WordPathStr);
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            String classname = args[0];
            class_term_num.put(classname,class_term_num.getOrDefault(classname,0.0)+1.0);
        }
        //计算每个类别里面出现的词条概率class-term prob
        reader = readFile(WordPathStr);
        while(reader.ready()){
            String line = reader.readLine();
            String[] args = line.split("\t");// 0：类，1：词条，2：词频
            double count = Double.valueOf(args[2]);
            String classname = args[0];
            String term = args[1];
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname,term);
            class_term_prob.put(map, (count+1)/(class_term_total.get(classname)+class_term_num.get(classname)));
        }

//        Utils.outInfo(class_prob.toString());
//        Utils.outInfo(class_term_total.toString());
//        Utils.outInfo(class_term_num.toString());
    }

    public String[] getNames() {
        return class_term_total.keySet().toArray(new String[0]);
    }
    public double conditionalProbabilityForClass(String content, String classname) throws Exception{
        // 计算一个文档属于某类的条件概率
        double result = 0;
//        Utils.outInfo("class_term_total:" + class_term_total);
//        Utils.outInfo("calc " + classname + ":" + class_term_total.get(classname));
        String[] words = content.split("\n");
        for(String word:words){
            Map<String,String> map = new HashMap<String, String>();
            map.put(classname, word);
            result += Math.log(class_term_prob.getOrDefault(map,
                    1.0/(class_term_total.get(classname)+class_term_num.get(classname))
                    )
            );
        }
        result += Math.abs(Math.log(class_prob.get(classname)));
        return result;
    }

    private BufferedReader readFile(String uri) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
        return reader;
    }
}