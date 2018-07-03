package com.gaiaworks.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by 唐哲
 * 2018-02-13 16:22
 */
public class GenerateData2 {
    
    public static String generateOneWorkStartRecord(long personId, String name) {
        String punchTime = GenerateData.generateWorkStartTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneWorkEndRecord(long personId, String name) {
        String punchTime = GenerateData.generateWorkEndTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneNoonStartRecord(long personId, String name) {
        String punchTime = GenerateData.generateNoonStartTime();
        return personId + "," + name + "," + punchTime;
    }

    public static String generateOneNoonEndRecord(long personId, String name) {
        String punchTime = GenerateData.generateNoonEndTime();
        return personId + "," + name + "," + punchTime;
    }

    public static void generateBatch() throws IOException {
        String filePath = "D:\\IdeaProjects\\Gaiaworks\\storm-train-demo\\data";
        File f = new File(filePath);
//        if (f.exists()) {
//            f.delete(); //存在则删除再创建
//            f.createNewFile();
//        } else {
//            f.createNewFile(); //不存在则创建
//        }

        BufferedWriter output = new BufferedWriter(new FileWriter(f, true));

        int i = 1;
        for(; i<=1; i++) {
            String name = GenerateData.generateName();
            output.write(generateOneWorkStartRecord(i, name) + "\r\n");
            output.write(generateOneWorkEndRecord(i, name) + "\r\n");
        }
        for(; i<=2; i++) {
            String name = GenerateData.generateName();
            output.write(generateOneWorkStartRecord(i, name) + "\r\n");
            output.write(generateOneNoonStartRecord(i, name) + "\r\n");
            output.write(generateOneNoonEndRecord(i, name) + "\r\n");
            output.write(generateOneWorkEndRecord(i, name) + "\r\n");
        }

        output.close();
    }

    public static void main(String[] args) throws IOException {
        generateBatch();
    }
    
}
