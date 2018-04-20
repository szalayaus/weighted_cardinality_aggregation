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

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;


import java.util.ArrayList;
import java.util.List;


public class WeightedCardinalityAggregationPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        ArrayList<AggregationSpec> aggregationSpecs = new ArrayList<>(1);
        aggregationSpecs.add(new AggregationSpec(WeightedCardinalityAggregationBuilder.NAME, WeightedCardinalityAggregationBuilder::new,
                WeightedCardinalityAggregationBuilder::parse)
                .addResultReader(InternalWeightedCardinality::new));
        return aggregationSpecs;
    }
}
