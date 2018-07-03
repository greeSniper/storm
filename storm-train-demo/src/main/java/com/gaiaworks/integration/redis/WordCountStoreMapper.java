package com.gaiaworks.integration.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by 唐哲
 * 2018-02-09 10:34
 */
public class WordCountStoreMapper implements RedisStoreMapper {

    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public WordCountStoreMapper() {
        this.description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, this.hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return this.description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getIntegerByField("count") + "";
    }

}
