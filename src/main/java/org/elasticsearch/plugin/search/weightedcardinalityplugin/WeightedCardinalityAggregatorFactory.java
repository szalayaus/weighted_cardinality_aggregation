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

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;


public class WeightedCardinalityAggregatorFactory
        extends ValuesSourceAggregatorFactory<ValuesSource, WeightedCardinalityAggregatorFactory> {

    private final Long precisionThreshold;
    private final String weightField;

    public WeightedCardinalityAggregatorFactory(String name, ValuesSourceConfig<ValuesSource> config, String weightField,
                                                Long precisionThreshold, SearchContext context,
                                                AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder,
                                                Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.weightField = weightField;
        this.precisionThreshold = precisionThreshold;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return new WeightedCardinalityAggregator(name, null, null, precision(), context, parent,
                pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
                                          List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        if (weightField == null) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Missing parameter [%s]",
                    WeightedCardinalityAggregationBuilder.WEIGHT_FIELD.getPreferredName()));
        }
        ValuesSourceConfig<?> config = ValuesSourceConfig.resolve(context.getQueryShardContext(),
                null, weightField, null, null, null, null);
        ValuesSource weightValuesSource = config.toValuesSource(context.getQueryShardContext());
        return new WeightedCardinalityAggregator(name, valuesSource, weightValuesSource, precision(), context, parent,
                pipelineAggregators, metaData);
    }

    private int precision() {
        return precisionThreshold == null
                ? HyperLogLogPlusPlus.DEFAULT_PRECISION
                : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}
