package com.gaiaworks.storm.word;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by 唐哲
 * 2018-02-06 16:58
 *
 * 使用Storm完成词频统计功能
 */
@Component("dataSourceSpout2")
public class DataSourceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 业务：
     * 1） 读取指定目录的文件夹下的数据:/Users/rocky/data/storm/wc
     * 2） 把每一行数据发射出去
     */
    @Override
    public void nextTuple() {
        //获取文件夹下的所有txt文件
        Collection<File> files = FileUtils.listFiles(new File("D:\\IdeaProjects\\Gaiaworks\\storm-train-demo\\wc"), new String[]{"txt"}, true);
        for(File file : files) {
            try {
                //获取文件所有行
                List<String> lines = FileUtils.readLines(file);
                //遍历每个文件的每一行
                for(String line : lines) {
                    //发射出去
                    this.collector.emit(new Values(line));
                }

                //TODO... 数据处理完之后，改名，否则一直重复执行
                FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
