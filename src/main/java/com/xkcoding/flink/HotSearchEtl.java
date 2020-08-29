package com.xkcoding.flink;

import cn.hutool.core.io.resource.ResourceUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;

import java.net.URL;

/**
 * <p>
 * 热门搜索 ETL
 * </p>
 *
 * @author 一珩（沈扬凯 yk.shen@tuya.com）
 * @since 2020/8/24 11:29
 */
@Slf4j
public class HotSearchEtl {
    @Data
    @AllArgsConstructor
    static class WordCount {
        private String word;
        private Integer count;
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 数据源：用户查询日志(SogouQ)
        // 下载地址：http://www.sogou.com/labs/resource/q.php
        // 数据格式：访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
        String filePath = "data/SogouQ.sample";
        URL resource = ResourceUtil.getResource(filePath);
        filePath = resource.getPath();

        DataSource<String> dataSource = environment.readTextFile(filePath, "GB2312");
        dataSource
                // 整理数据，只抽取：查询词
                .map((MapFunction<String, WordCount>) source -> new WordCount(source.split("\t")[2], 1))
                // 按照查询词分组
                .groupBy((KeySelector<WordCount, String>) WordCount::getWord)
                // 将相同查询词的数量累积
                .reduce((ReduceFunction<WordCount>) (value1, value2) -> new WordCount(value1.getWord(),
                        value1.getCount() + value2.getCount()))
                // 按照数量倒序
                .sortPartition((KeySelector<WordCount, Integer>) WordCount::getCount, Order.DESCENDING)
                // 取前 100
                .first(100)
                .print();


    }
}
