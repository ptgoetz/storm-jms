package org.apache.storm.jms.spout;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

public class MockTopologyContext extends TopologyContext {

    public MockTopologyContext() {

        super(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        return metric;
    }
}
