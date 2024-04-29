package org.myorg.quickstart;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyFirstSnapshotFunction extends KeyedProcessFunction<String, TimestampedWordCount, TimestampedWordCount> {
    public static final ValueStateDescriptor<TimestampedWordCount> TIMESTAMPED_WORD_COUNTING_FUNCTION_STATE = new ValueStateDescriptor<>("timestampedWordCountingFunctionState", TimestampedWordCount.class);
    private ValueState<TimestampedWordCount> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(TIMESTAMPED_WORD_COUNTING_FUNCTION_STATE);
    }

    @Override
    public void processElement(TimestampedWordCount timestampedWordCount, KeyedProcessFunction<String, TimestampedWordCount, TimestampedWordCount>.Context context, Collector<TimestampedWordCount> collector) throws Exception {
        state.update(timestampedWordCount);
    }
}
