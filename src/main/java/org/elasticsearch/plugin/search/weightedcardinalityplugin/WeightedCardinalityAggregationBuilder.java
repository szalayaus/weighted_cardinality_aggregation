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

import java.io.IOException;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;


public final class WeightedCardinalityAggregationBuilder
        extends ValuesSourceAggregationBuilder.LeafOnly<ValuesSource, WeightedCardinalityAggregationBuilder> {

    public static final String NAME = "weighted_cardinality";

    public static final ParseField WEIGHT_FIELD = new ParseField("weight_field");
    public static final ParseField PRECISION_THRESHOLD_FIELD = new ParseField("precision_threshold");

    private static final ObjectParser<WeightedCardinalityAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(WeightedCardinalityAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, false);
        PARSER.declareString(WeightedCardinalityAggregationBuilder::weightField, WeightedCardinalityAggregationBuilder.WEIGHT_FIELD);
        PARSER.declareLong(WeightedCardinalityAggregationBuilder::precisionThreshold,
                WeightedCardinalityAggregationBuilder.PRECISION_THRESHOLD_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new WeightedCardinalityAggregationBuilder(aggregationName, null), null);
    }

    private String weightField = null;
    private Long precisionThreshold = null;

    public WeightedCardinalityAggregationBuilder(String name, ValueType targetValueType) {
        super(name, ValuesSourceType.ANY, targetValueType);
    }

    public WeightedCardinalityAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.ANY);
        if (in.readBoolean()) {
            weightField = in.readString();
        }

        if (in.readBoolean()) {
            precisionThreshold = in.readLong();
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        boolean hasWeightField = weightField != null;
        out.writeBoolean(hasWeightField);
        if (hasWeightField) {
            out.writeString(weightField);
        }

        boolean hasPrecisionThreshold = precisionThreshold != null;
        out.writeBoolean(hasPrecisionThreshold);
        if (hasPrecisionThreshold) {
            out.writeLong(precisionThreshold);
        }
    }

    public WeightedCardinalityAggregationBuilder weightField(String weightField) {
        this.weightField = weightField;
        return this;
    }

    public WeightedCardinalityAggregationBuilder precisionThreshold(long precisionThreshold) {
        if (precisionThreshold < 0) {
            throw new IllegalArgumentException(
                    "[precisionThreshold] must be greater than or equal to 0. Found [" + precisionThreshold + "] in [" + name + "]");
        }
        this.precisionThreshold = precisionThreshold;
        return this;
    }

    public Long precisionThreshold() {
        return precisionThreshold;
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    @Override
    protected WeightedCardinalityAggregatorFactory innerBuild(SearchContext context, ValuesSourceConfig<ValuesSource> config,
                    AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new WeightedCardinalityAggregatorFactory(name, config, weightField, precisionThreshold, context, parent,
                subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(WEIGHT_FIELD.getPreferredName(), weightField);
        if (precisionThreshold != null) {
            builder.field(PRECISION_THRESHOLD_FIELD.getPreferredName(), precisionThreshold);
        }
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return 0;
    }

    @Override
    protected boolean innerEquals(Object obj) {
        return true;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
