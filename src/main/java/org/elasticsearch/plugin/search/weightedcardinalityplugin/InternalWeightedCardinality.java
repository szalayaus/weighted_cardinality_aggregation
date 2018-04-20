/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.search.weightedcardinalityplugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalWeightedCardinality extends InternalNumericMetricsAggregation.MultiValue implements WeightedCardinality {
    private final HyperLogLogPlusPlus counts;
    private final HyperLogLogPlusPlus weights;

    enum Metrics {

        count, weight;

        public static InternalWeightedCardinality.Metrics resolve(String name) {
            return InternalWeightedCardinality.Metrics.valueOf(name);
        }
    }

    InternalWeightedCardinality(String name, HyperLogLogPlusPlus counts, HyperLogLogPlusPlus weights,
                                List<PipelineAggregator> pipelineAggregators,
                                Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.counts = counts;
        this.weights = weights;
    }


    public InternalWeightedCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            counts = null;
        }

        if (in.readBoolean()) {
            weights = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            weights = null;
        }
    }

    @Override
    public double value(String name) {
        Metrics metrics = Metrics.valueOf(name);
        switch (metrics) {
            case count: return this.counts != null ? (double)this.counts.cardinality(0) : 0;
            case weight: return this.weights != null ? (double)this.weights.cardinality(0) : 0;
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in weighted cardinality aggregation");
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }

        if (weights != null) {
            out.writeBoolean(true);
            weights.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return WeightedCardinalityAggregationBuilder.NAME;
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public long getWeightValue() {
        return weights == null ? 0 : weights.cardinality(0);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalWeightedCardinality reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalWeightedCardinality cardinality = (InternalWeightedCardinality) aggregation;
            if (cardinality.counts != null || cardinality.weights != null) {
                if (reduced == null) {
                    HyperLogLogPlusPlus counts = cardinality.counts != null
                            ? new HyperLogLogPlusPlus(cardinality.counts.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1)
                            : null;
                    HyperLogLogPlusPlus weights = cardinality.weights != null
                            ? new HyperLogLogPlusPlus(cardinality.weights.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1)
                            : null;
                    reduced = new InternalWeightedCardinality(name, counts, weights, pipelineAggregators(), getMetaData());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalWeightedCardinality other) {
        assert (counts != null || weights != null) && other != null;
        if (counts != null) {
            counts.merge(0, other.counts, 0);
        }
        if (weights != null) {
            weights.merge(0, other.weights, 0);
        }
    }

    static class Fields {
        public static final String COUNT = "count";
        public static final String WEIGHT = "weight";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.COUNT, getValue());
        builder.field(Fields.WEIGHT, getWeightValue());
        return builder;
    }

    @Override
    public boolean doEquals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        final InternalWeightedCardinality other = (InternalWeightedCardinality) o;

        return counts.equals(0, other.counts) && weights.equals(0, other.weights);
    }

    @Override
    public int doHashCode() {
        int result = 0;
        if (counts != null) {
            result = counts.hashCode(0);
        }
        if (weights != null) {
            result += 31 * weights.hashCode(0);
        }

        return result;
    }
}