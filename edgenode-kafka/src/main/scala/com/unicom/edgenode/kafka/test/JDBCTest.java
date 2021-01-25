package com.unicom.edgenode.kafka.test;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class JDBCTest {
    public static void main(String[] args) {
        String content ="010|150998294|40.4883603286|111.7707376711|8|V0150100|010|6|589837|22|1|内蒙古自治区|呼和浩特市|和林格尔县|150123|2020-12-07 15:04:14\n" +
                "010|150998295|40.4883603286|111.7707376711|8|V0150100|010|6|589837|23|1|内蒙古自治区|呼和浩特市|和林格尔县|150123|2020-12-07 15:04:14\n" +
                "010|150998296|40.4883603286|111.7707376711|8|V0150100|010|6|589837|24|1|内蒙古自治区|呼和浩特市|和林格尔县|150123|2020-12-0715:04:14";

        String path="D://test001.txt";
        FileWriter fw= null;
        try {
            fw = new FileWriter(path,true);
            BufferedWriter bw=new BufferedWriter(fw);
//            bw.newLine();
            bw.write(content);
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
