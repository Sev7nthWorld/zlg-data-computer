package cn.com.zlg.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MyFlatMapFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Long>> {
    private transient ValueState<Long> leastValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("leastValue", Long.class);
        leastValueState = getRuntimeContext().getState(descriptor);
        StateTtlConfig config = StateTtlConfig.newBuilder(Time.seconds(100)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
        descriptor.enableTimeToLive(config);
    }


    @Override
    public void flatMap(Tuple2<String, Integer> stringLongTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
        Tuple2<String, Integer> element = stringLongTuple2;
        Long leastValue = 0L;
        if(leastValueState.value() == null)
            leastValue = Long.valueOf(stringLongTuple2.f1);
        System.out.println("--------leastValue----------" + leastValue);
        if (element.f1 > leastValue)
            collector.collect(new Tuple2<String, Long>(element.f0, leastValue));
        else {
            leastValueState.update(Long.valueOf(element.f1));
            collector.collect(new Tuple2<String, Long>(element.f0, Long.valueOf(element.f1)));
        }
    }
}
