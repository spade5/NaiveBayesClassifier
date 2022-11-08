package org.spade5.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TestHDFS {
    public static void main(String[] args) throws Exception {
        readFile("/output_doc/part-r-00000");
        readFile("/output_word/part-r-00000");
    }

    public static void readFile(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsin= fs.open(new Path(path));
        BufferedReader br =null;
        String line ;
        try{
            System.out.println("start print " + path);
            br = new BufferedReader(new InputStreamReader(fsin));
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            System.out.println("end print " + path);
        }finally {
            br.close();
        }
    }

    public static List getFilesUnderFolder(FileSystem fs, Path folderPath, String pattern) throws IOException {
        List paths = new ArrayList();
        if (fs.exists(folderPath)) {
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileStatus fileStatu = fileStatus[i];
                if (!fileStatu.isDirectory()) {//仅仅要文件
                    Path oneFilePath = fileStatu.getPath();
                    if (pattern == null) {
                        paths.add(oneFilePath);
                    } else {
                        if (oneFilePath.getName().contains(pattern)) {
                            paths.add(oneFilePath);
                        }
                    }
                }
            }
        }

        return paths;
    }
}
