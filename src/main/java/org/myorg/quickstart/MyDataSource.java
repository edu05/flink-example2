package org.myorg.quickstart;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class MyDataSource extends DataGeneratorSource<MyRecord> {
    public MyDataSource() {
        super(new GeneratorFunction<Long, MyRecord>() {
            private int count = 0;

            @Override
            public MyRecord map(Long x) throws Exception {
                count++;

                String newFeatureValue = "" + count;
                if (count % 2 == 0 && count < 10) {
                    return new MyRecord("aaaa", count % 4 == 0 ? newFeatureValue : null, count % 4 == 0 ? null : newFeatureValue);
                } else {
                    return new MyRecord("bbbb", (count - 1) % 4 == 0 ? newFeatureValue : null, (count - 1) % 4 == 0 ? null : newFeatureValue);

                }
            }
        }, 1000, RateLimiterStrategy.perSecond(1), TypeInformation.of(new TypeHint<MyRecord>() {
        }));
    }


}
