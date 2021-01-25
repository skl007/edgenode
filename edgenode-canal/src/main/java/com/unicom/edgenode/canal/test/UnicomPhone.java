package com.unicom.edgenode.canal.test;



import com.unicom.edgenode.canal.common.Const;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Pattern;

import static java.nio.file.Files.newBufferedReader;

public class UnicomPhone {

    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("D:\\hbsjzfy\\mijiepeople.txt"));

        String line = null;
        LinkedHashMap<String, String> maps = new LinkedHashMap<>();
        ArrayList<String> strings = new ArrayList<>();
        while((line = br.readLine())!= null){
            String[] split = line.split(",");
            if(split.length ==2){
                String key = split[0];
                String value = split[1];
                maps.put(key,value);
            }
        }
        //释放资源
        br.close();
        //遍历集合
        for(Map.Entry<String, String> entry: maps.entrySet())
        {
            String chinaMobilePhoneNum = isChinaMobilePhoneNum(entry.getValue());
                System.out.println(entry.getKey()+ ","+entry.getValue() + ","+chinaMobilePhoneNum);
        }


    }

    /**
     * 查询电话属于哪个运营商
     *
     * @param tel 手机号码
     * @return 0：不属于任何一个运营商，1:移动，2：联通，3：电信
     */
    public static String isChinaMobilePhoneNum(String tel) {
        boolean b1 = tel == null || tel.trim().equals("") ? false : match(Const.CHINA_MOBILE_PATTERN, tel);
        if (b1) {
            return "移动";
        }
        b1 = tel == null || tel.trim().equals("") ? false : match(Const.CHINA_UNICOM_PATTERN, tel);
        if (b1) {
            return "联通";
        }
        b1 = tel == null || tel.trim().equals("") ? false : match(Const.CHINA_TELECOM_PATTERN, tel);
        if (b1) {
            return "电信";
        }
        return "非联通";
    }

    /**
     * 匹配函数
     * @param regex
     * @param tel
     * @return
     */
    private static boolean match(String regex, String tel) {
        return Pattern.matches(regex, tel);
    }








































    //判断是否是联通手机号码

   /* public boolean isLTMobile(String mobile){
        if(mobile!=null){
            var reg=/^(0|86|17951)?(13[012]|15[56]|17[6]|18[56]|14[5])[0-9]{8}$/;

            if(reg.test(mobile)){//联通手机号码
                return true;
            }

        }

        retrun false;

    }*/


  /*  //判断是否是手机号码

    public boolean isMobile(String mobile){
        String  regMobile = "/^1[3|4|5|6|7|8|9][0-9]{1}[0-9]{8}$/";

        if(!mobile || !regMobile.test(mobile)){
            //请输入正确的手机号！
            return false;
        }
        return true;

    }*/
}
