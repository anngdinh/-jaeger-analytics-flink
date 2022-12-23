package com.example.deserializer;

import com.example.model.SpanModel;
import com.jsoniter.JsonIterator;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class SpanDeserializer extends AbstractDeserializationSchema<SpanModel> {
    @Override
    public SpanModel deserialize(byte[] bytes) {
        try {
            return JsonIterator.deserialize(bytes, SpanModel.class);
        } catch (Exception ex) {
            return null;
        }
    }
}

