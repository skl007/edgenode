package com.unicom.edgenodech.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.unicom.edgenodech.bean.Code;
import com.unicom.edgenodech.bean.DataResponse;
import com.unicom.edgenodech.util.JDBCUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * http数据服务，亦可用于rcp等同类型服务
 *
 * @version $Id: HttpServerController.java, v 0.1 2019年4月19日 上午9:53:14 Exp $
 */
@RestController
public class HttpServerController {

    @Autowired
    JDBCUtils jdbcUtils;

    @PostMapping("/base/station/hebei")
    public DataResponse baseStationHebei(@Validated @RequestBody String body) {
        String password = "three.*?";
        List<Map<String, Object>> datas = Lists.newArrayList();
        List<String> cols = Lists.newArrayList("lac","ci","lat","lon","rev_sta","area_id","prov_id","net_type","enodeb_id","cell_id","cell_type","prov_desc","area_desc","district_desc","district_id","update_time");
        String[] str = body.split("\r\n");

        for (int i = 0; i < str.length; i++) {
            String line = str[i];
            String[] line_str = line.split("[,]");
            if (line_str.length != cols.size()) {
                return new DataResponse(Code.FAIL, "入库信息与配置不匹配");
            }
            AtomicInteger index = new AtomicInteger(0);

            Map<String, Object> m = Maps.newHashMap();

            Stream<String> stream = cols.stream();
            cols.stream().forEach(c -> {
                m.put(c, line_str[index.getAndIncrement()]);
            });

            //将时间精确到秒,clickhouse的DateTime类型的数据只保留到秒,其他格式无法存入
            String update_time = (String) m.get("update_time");
            if (update_time.length() == 18) {
                String start = update_time.substring(0, 10);
                String end = update_time.substring(10, 18);
                update_time = start+" " + end;
                m.replace("update_time", update_time);
            }

            try {
                datas.add(m);
                if (i % 10000 == 0 && i != 0) {
                    System.out.println(i);
                    jdbcUtils.insertAllByList("ods_base_station_hebei", datas, cols);
                    datas.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            jdbcUtils.insertAllByList("ods_base_station_hebei", datas, cols);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new DataResponse("ok");

    }




}
