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
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        List paths = getFilesUnderFolder(fs, new Path("/"), null);

        FSDataInputStream fsin= fs.open(new Path("/output_doc/part-r-00000"));

        BufferedReader br =null;

        String line ;

        try{
            System.out.println("start print output_doc");
            br = new BufferedReader(new InputStreamReader(fsin));

            while ((line = br.readLine()) != null) {

                System.out.println(line);

            }
            System.out.println("end print output_doc");
        }finally{

            br.close();

        }

        System.out.println("");
        for (int i = 0; i < paths.size(); i++) {
            System.out.println(paths.get(i));
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
