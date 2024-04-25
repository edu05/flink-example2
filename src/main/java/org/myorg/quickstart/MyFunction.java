package org.myorg.quickstart;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyFunction extends KeyedProcessFunction<String, MyRecord, MyRecord> {

    private transient ValueState<MyRecordWithTimestamp> currentRecordValueState;


    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<MyRecordWithTimestamp> descriptor =
                new ValueStateDescriptor<>(
                        "records", // the state name
                        TypeInformation.of(new TypeHint<MyRecordWithTimestamp>() {
                        }), // type information
                        new MyRecordWithTimestamp(new MyRecord(), 0l)); // default value of the state, if nothing was set
        currentRecordValueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(MyRecord recordEvent, KeyedProcessFunction<String, MyRecord, MyRecord>.Context context, Collector<MyRecord> collector) throws Exception {
        MyRecordWithTimestamp currentRecord = currentRecordValueState.value();
        MyRecord newRecord = new MyRecord(recordEvent.clientId,
                recordEvent.feature1 == null ? currentRecord.myRecord.feature1 : recordEvent.feature1,
                recordEvent.feature2 == null ? currentRecord.myRecord.feature2 : recordEvent.feature2
        );
        currentRecordValueState.update(new MyRecordWithTimestamp(newRecord, context.timestamp()));
        collector.collect(newRecord);

        context.timerService().deleteEventTimeTimer(currentRecord.timestamp + 20 * 1000);
        context.timerService().registerEventTimeTimer(context.timestamp() + 20 * 1000);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, MyRecord, MyRecord>.OnTimerContext ctx, Collector<MyRecord> out) throws Exception {
        if (currentRecordValueState.value().timestamp + 20 * 1000 == timestamp) {
            MyRecord newEmptyRecord = new MyRecord(ctx.getCurrentKey(), null, null);
            out.collect(newEmptyRecord);
            currentRecordValueState.update(new MyRecordWithTimestamp(newEmptyRecord, timestamp));
            ctx.timerService().registerEventTimeTimer(timestamp + 20 * 1000);
        } else {
            System.out.println("noop");
        }
    }
}
