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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESIntegTestCase;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

@ESIntegTestCase.SuiteScopeTestCase
public class WeightedCardinalityAggregationTests extends ESIntegTestCase {

    static ArrayList<Tuple<String, Integer>> deduped = new ArrayList<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(WeightedCardinalityAggregationPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(WeightedCardinalityAggregationPlugin.class);
    }

    class Tuple<X, Y> {
        public final X x;
        public final Y y;
        Tuple(X x, Y y) {
            this.x = x;
            this.y = y;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        List<IndexRequestBuilder> builders ;

        prepareCreate("idx_empty_cardinality")
                .addMapping("doc", "name", "type=keyword", "id", "type=keyword", "size", "type=integer")
                .execute()
                .actionGet();

        prepareCreate("idx_cardinality")
            .addMapping("doc", "id", "type=keyword", "size", "type=integer")
            .execute()
            .actionGet();

        Random random = Randomness.get();
        builders = new ArrayList<>();
        int numDocs = 10000;
        for (int i = 0; i < numDocs; i++) {
            boolean shouldDedupe = random.nextInt(5) == 0;
            int idx;
            if (shouldDedupe) {
                try {
                    idx = random.nextInt(deduped.size());
                } catch (Exception ex) {
                    idx = -1;
                }
            }
            else {
                idx = -1;
            }
            int multiplicity;
            String docId;
            if (idx < 0) {
                multiplicity = 1 + random.nextInt(100 - 1);
                docId = String.format(Locale.ENGLISH, "Docs/%1d", i + 1);
                deduped.add(new Tuple<>(docId, multiplicity));
            }
            else {
                Tuple<String, Integer> tpl = deduped.get(idx);
                docId = tpl.x;
                multiplicity = tpl.y;
            }
            builders.add(client().prepareIndex("idx_cardinality", "doc", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("id", docId)
                    .field("size", multiplicity)
                    .endObject()));
        }
        builders.add(client().prepareIndex("idx_empty_cardinality", "doc", "1").setSource(jsonBuilder()
                .startObject()
                .field("name", "empty")
                .endObject()));
        indexRandom(true, builders);

        ensureSearchable();
    }

    public void testMissingParameter() throws Exception {
        try {
            SearchResponse searchResponse = client().prepareSearch("idx_cardinality")
                    .setQuery(matchAllQuery())
                    .addAggregation(new WeightedCardinalityAggregationBuilder("card", ValueType.STRING).field("id"))
                    //.addAggregation(new CardinalityAggregationBuilder("card", ValueType.STRING).field("id"))
                    .execute().actionGet();
            assert false;
        }
        catch (SearchPhaseExecutionException ex) {
            Throwable cause = ex.getCause();
            assert cause instanceof IllegalArgumentException || cause.getCause() instanceof IllegalArgumentException;
        }
    }

    public void testInvalidParameter() throws Exception {
        try {
            SearchResponse searchResponse = client().prepareSearch("idx_cardinality")
                    .setQuery(matchAllQuery())
                    .addAggregation(new WeightedCardinalityAggregationBuilder("card", ValueType.STRING)
                            .field("id").weightField("id"))
                    .execute().actionGet();
            assert false;
        }
        catch (SearchPhaseExecutionException ex) {
            Throwable cause = ex.getCause();
            assert cause instanceof NumberFormatException || cause.getCause() instanceof NumberFormatException;
        }
    }
    public void testEmptyCardinality() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_empty_cardinality")
                .setQuery(matchAllQuery())
                .addAggregation(new WeightedCardinalityAggregationBuilder("card", ValueType.STRING).field("id").weightField("size"))
                .execute().actionGet();
        Aggregations aggs = searchResponse.getAggregations();
        for (Aggregation agg: aggs) {
            assert agg.getName().equals("card");
            String aggType = agg.getType();
            assert aggType.equals(WeightedCardinalityAggregationBuilder.NAME);
            InternalWeightedCardinality cardinality = (InternalWeightedCardinality) agg;
            assert cardinality.getValue() == 0;
            assert cardinality.getWeightValue() == 0;
        }
    }

    public void testCardinality() throws Exception {
       SearchResponse searchResponse = client().prepareSearch("idx_cardinality")
            .setQuery(matchAllQuery())
            .addAggregation(new WeightedCardinalityAggregationBuilder("card", ValueType.STRING).field("id").weightField("size"))
                //.addAggregation(new CardinalityAggregationBuilder("card", ValueType.STRING).field("id"))
            .execute().actionGet();

        long weight = 0;
        for (Tuple<String, Integer> tpl: deduped) {
            weight += tpl.y;
        }

        Aggregations aggs = searchResponse.getAggregations();
        for (Aggregation agg: aggs) {
            assert agg.getName().equals("card");

            String aggType = agg.getType();
            if (aggType.equals(WeightedCardinalityAggregationBuilder.NAME)) {
                assert aggType.equals(WeightedCardinalityAggregationBuilder.NAME);
                InternalWeightedCardinality cardinality = (InternalWeightedCardinality)agg;

                double ratio = deduped.size() > 0 ? Math.abs(cardinality.getValue() - deduped.size()) * 1.0 / deduped.size() : 0;
                assert ratio < 0.02;

                double weightRatio = weight > 0 ? Math.abs(cardinality.getWeightValue() - weight) * 1.0 / weight : 0;
                assert weightRatio < 0.02;
            }
            else if (aggType.equals(CardinalityAggregationBuilder.NAME)) {
                InternalCardinality cardinality = (InternalCardinality)agg;

                double ratio = deduped.size() > 0 ? Math.abs(cardinality.getValue() - deduped.size()) * 1.0 / deduped.size() : 0;
                assert ratio < 0.02;
            }

        }

    }
}
