package org.myorg.quickstart;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimestampedWordCountingFunction extends KeyedProcessFunction<String, TimestampedWord, TimestampedWordCount> {

    public static final ValueStateDescriptor<TimestampedWordCount> TIMESTAMPED_WORD_COUNTING_FUNCTION_STATE = new ValueStateDescriptor<>("timestampedWordCountingFunctionState", TimestampedWordCount.class);
    private ValueState<TimestampedWordCount> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(TIMESTAMPED_WORD_COUNTING_FUNCTION_STATE);
    }

    @Override
    public void processElement(TimestampedWord timestampedWord, Context ctx, Collector<TimestampedWordCount> outputStream) throws Exception {
        TimestampedWordCount timestampedWordCount = state.value();
        if (timestampedWordCount == null) {
            timestampedWordCount = new TimestampedWordCount(timestampedWord, 0);
        }

        TimestampedWordCount updatedTimestampedWordCount = new TimestampedWordCount(timestampedWord, timestampedWordCount.getCount() + 1);
        state.update(updatedTimestampedWordCount);
        outputStream.collect(updatedTimestampedWordCount);
    }

//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//    }

//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        ValueState<TimestampedWordCount> state = context.getKeyedStateStore().getState(TIMESTAMPED_WORD_COUNTING_FUNCTION_STATE);
//        state.update(new TimestampedWordCount(new TimestampedWord("edu", 1), 3));
//    }
}