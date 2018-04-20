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

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.cardinality.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

/**
 * An aggregator that computes approximate counts of unique values and the sum of their approximate weights
 */
public class WeightedCardinalityAggregator extends NumericMetricsAggregator.MultiValue {

    private final int precision;
    private final ValuesSource valuesSource;
    private final ValuesSource weightValuesSource;

    // Expensive to initialize, so we only initialize it when we have an actual value source
    @Nullable
    private HyperLogLogPlusPlus counts;
    @Nullable
    private HyperLogLogPlusPlus weights;

    private Collector collector;

    public WeightedCardinalityAggregator(String name, ValuesSource valuesSource, ValuesSource weightValuesSource,
                                         int precision, SearchContext context, Aggregator parent,
                                         List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.weightValuesSource = weightValuesSource;
        this.precision = precision;
        this.counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        this.weights = weightValuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public boolean hasMetric(String name) {
        return false;   // TODO
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);   // TODO
    }

    private Collector pickCollector(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return new EmptyCollector();
        }

        WeightValues weightValues = getWeights(ctx);

        if (valuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) valuesSource;
            MurmurHash3Values hashValues = source.isFloatingPoint()
                    ? MurmurHash3Values.hash(source.doubleValues(ctx)) : MurmurHash3Values.hash(source.longValues(ctx));
            return new DirectCollector(counts, hashValues, weights, weightValues);
        }

        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
            final SortedSetDocValues ordinalValues = source.ordinalsValues(ctx);
            final long maxOrd = ordinalValues.getValueCount();
            if (maxOrd == 0) {
                return new EmptyCollector();
            }

            final long ordinalsMemoryUsage = OrdinalsCollector.memoryOverhead(maxOrd);
            final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
            // only use ordinals if they don't increase memory usage by more than 25%
            if (ordinalsMemoryUsage < countsMemoryUsage / 4) {
                return new OrdinalsCollector(counts, ordinalValues, context.bigArrays(), weights, weightValues);
            }
        }

        return new DirectCollector(counts, MurmurHash3Values.hash(valuesSource.bytesValues(ctx)), weights, weightValues);
    }

    private WeightValues getWeights(LeafReaderContext ctx) throws IOException {
        if (weightValuesSource == null) {
            return null;
        }

        if (weightValuesSource instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric source = (ValuesSource.Numeric) weightValuesSource;
            if (source.isFloatingPoint()) {
                return WeightValues.get(source.doubleValues(ctx));
            }
            return WeightValues.get(source.longValues(ctx));
        }
        if (weightValuesSource instanceof ValuesSource.Bytes) {
            return WeightValues.get(weightValuesSource.bytesValues(ctx));
        }
        return null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        postCollectLastCollector();

        collector = pickCollector(ctx);
        return collector;
    }

    private void postCollectLastCollector() throws IOException {
        if (collector != null) {
            try {
                collector.postCollect();
                collector.close();
            } finally {
                collector = null;
            }
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        postCollectLastCollector();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        HyperLogLogPlusPlus copyCounts = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copyCounts.merge(0, counts, owningBucketOrdinal);
        HyperLogLogPlusPlus copyWeights = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copyWeights.merge(0, weights, owningBucketOrdinal);
        return new InternalWeightedCardinality(name, copyCounts, copyWeights, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalWeightedCardinality(name, null, null, pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts, weights, collector);
    }

    private abstract static class Collector extends LeafBucketCollector implements Releasable {

        public abstract void postCollect() throws IOException;

        public long maxWeight(Collection<Long> longValues) {
            long maxWeight = 0;
            for (long w: longValues) {
                if (w > maxWeight) {
                    maxWeight = w;
                }
            }
            return maxWeight;
        }

    }

    private static class EmptyCollector extends Collector {

        @Override
        public void collect(int doc, long bucketOrd) {
            // no-op
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static class DirectCollector extends Collector {

        private final MurmurHash3Values countsHashes;
        private final HyperLogLogPlusPlus counts;
        private final WeightValues weightValues;
        private final HyperLogLogPlusPlus weights;

        DirectCollector(HyperLogLogPlusPlus counts, MurmurHash3Values countsValues, HyperLogLogPlusPlus weights,
                        WeightValues weightValues) {
            this.counts = counts;
            this.countsHashes = countsValues;
            this.weights = weights;
            this.weightValues = weightValues;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (countsHashes.advanceExact(doc)) {
                final int valueCount = countsHashes.count();
                ArrayList<Long> longValues = weightValues.advanceExact(doc) ? weightValues.values() : null;

                final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
                for (int i = 0; i < valueCount; ++i) {
                    long l = countsHashes.nextValue();
                    counts.collect(bucketOrd, l);
                    if (longValues != null) {
                        long maxWeight = maxWeight(longValues);
                        for (int w = 0; w < maxWeight; w++) {
                            String v = Long.toHexString(l) + "/" + w;
                            byte[] bytes = v.getBytes(Charset.forName("utf-8"));
                            org.elasticsearch.common.hash.MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
                            weights.collect(bucketOrd, hash.h1);
                        }
                    }
                }
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            // no-op
        }

    }

    private static class OrdinalsCollector extends Collector {

        private static final long SHALLOW_FIXEDBITSET_SIZE = RamUsageEstimator.shallowSizeOfInstance(FixedBitSet.class);

        /**
         * Return an approximate memory overhead per bucket for this collector.
         */
        public static long memoryOverhead(long maxOrd) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF + SHALLOW_FIXEDBITSET_SIZE + (maxOrd + 7) / 8; // 1 bit per ord
        }

        private final BigArrays bigArrays;
        private final SortedSetDocValues values;
        private final int maxOrd;
        private final HyperLogLogPlusPlus counts;
        private ObjectArray<FixedBitSet> visitedOrds;
        private final WeightValues weightValues;
        private final HyperLogLogPlusPlus weights;
        private HashMap<Integer, HashSet<java.lang.Long>> weightLongValues;

        OrdinalsCollector(HyperLogLogPlusPlus counts, SortedSetDocValues values,
                          BigArrays bigArrays, HyperLogLogPlusPlus weights, WeightValues weightValues) {
            if (values.getValueCount() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            maxOrd = (int) values.getValueCount();
            this.bigArrays = bigArrays;
            this.counts = counts;
            this.values = values;
            this.weights = weights;
            this.weightValues = weightValues;
            visitedOrds = bigArrays.newObjectArray(1);
            weightLongValues = new HashMap<>();
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
            FixedBitSet bits = visitedOrds.get(bucketOrd);
            if (bits == null) {
                bits = new FixedBitSet(maxOrd);
                visitedOrds.set(bucketOrd, bits);
            }
            if (values.advanceExact(doc)) {
                ArrayList<Long> wv;
                if (weightValues.advanceExact(doc)) {
                    wv = weightValues.values();
                }
                else {
                    wv = null;
                }

                for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                    bits.set((int) ord);
                    if (wv != null) {
                        HashSet<java.lang.Long> longValues = weightLongValues.computeIfAbsent((int) ord, k -> new HashSet<>());
                        longValues.addAll(wv);
                    }
                }
            }
        }

        @Override
        public void postCollect() throws IOException {
            final FixedBitSet allVisitedOrds = new FixedBitSet(maxOrd);
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                final FixedBitSet bits = visitedOrds.get(bucket);
                if (bits != null) {
                    allVisitedOrds.or(bits);
                }
            }

            final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();
            try (LongArray hashes = bigArrays.newLongArray(maxOrd, false)) {
                for (int ord = allVisitedOrds.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS; ord = ord + 1 < maxOrd
                        ? allVisitedOrds.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                    final BytesRef value = values.lookupOrd(ord);
                    org.elasticsearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    hashes.set(ord, hash.h1);
                }

                for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                    final FixedBitSet bits = visitedOrds.get(bucket);
                    if (bits != null) {
                        for (int ord = bits.nextSetBit(0); ord < DocIdSetIterator.NO_MORE_DOCS; ord = ord + 1 < maxOrd
                                ? bits.nextSetBit(ord + 1) : DocIdSetIterator.NO_MORE_DOCS) {
                            long l = hashes.get(ord);
                            counts.collect(bucket, l);
                            if (weightLongValues.containsKey(ord)) {
                                HashSet<Long> longValues = weightLongValues.get(ord);
                                if (longValues.size() > 0) {
                                    long maxWeight = maxWeight(longValues);
                                    for (int w = 0; w < maxWeight; w++) {
                                        String v = Long.toHexString(l) + "/" + Long.toHexString(w);
                                        byte[] bytes = v.getBytes(Charset.forName("utf-8"));
                                        org.elasticsearch.common.hash.MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
                                        weights.collect(bucket, hash.h1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(visitedOrds);
        }

    }

    abstract static class WeightValues {
        public abstract boolean advanceExact(int docId) throws IOException;
        public abstract ArrayList<java.lang.Long> values() throws IOException;

        public static WeightValues get(SortedNumericDoubleValues values) { return new WeightValues.Double(values); }

        public static WeightValues get(SortedNumericDocValues values) { return new WeightValues.Long(values); }

        public static WeightValues get(SortedBinaryDocValues values) { return new WeightValues.Bytes(values); }

        private static class Long extends WeightValues {

            private final SortedNumericDocValues values;

            Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public ArrayList<java.lang.Long> values() throws IOException {
                ArrayList<java.lang.Long> longValues = new ArrayList<>(values.docValueCount());
                for (int i = 0; i < longValues.size(); i++) {
                    longValues.add(values.nextValue());
                }
                return longValues;
            }
        }

        private static class Double extends WeightValues {

            private final SortedNumericDoubleValues values;

            Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public ArrayList<java.lang.Long> values() throws IOException {
                ArrayList<java.lang.Long> longValues = new ArrayList<>(values.docValueCount());
                for (int i = 0; i < longValues.size(); i++) {
                    final long longValue = (long)values.nextValue();
                    longValues.add(longValue);
                }
                return longValues;
            }
        }

        private static class Bytes extends WeightValues {

            private final SortedBinaryDocValues values;

            Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public ArrayList<java.lang.Long> values() throws IOException {
                int docValueCount = values.docValueCount();
                ArrayList<java.lang.Long> longValues = new ArrayList<>(docValueCount);
                for (int i = 0; i < docValueCount; i++) {
                    final BytesRef bytes = values.nextValue();
                    longValues.addAll(convert(bytes));
                }
                return longValues;
            }

            private ArrayList<java.lang.Long> convert(BytesRef bytesRef) {
                final long weight = java.lang.Long.valueOf(bytesRef.utf8ToString());
                return new ArrayList<java.lang.Long>() {{ add(weight); }};
            }

            private ArrayList<java.lang.Long> convertLongBytes(BytesRef bytesRef) {
                final ByteArrayDataInput in = new ByteArrayDataInput();
                in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                ArrayList<java.lang.Long> longs = new ArrayList<>();
                if (!in.eof()) {
                    long previousValue = ByteUtils.zigZagDecode(ByteUtils.readVLong(in));
                    longs.add(previousValue);
                    while (!in.eof()) {
                        previousValue = previousValue + ByteUtils.readVLong(in);
                        longs.add(previousValue);
                    }
                }
                return longs;
            }
        }
    }

    /**
     * Representation of a list of hash values. There might be dups and there is no guarantee on the order.
     */
    abstract static class MurmurHash3Values {

        public abstract boolean advanceExact(int docId) throws IOException;

        public abstract int count();

        public abstract long nextValue() throws IOException;

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each double value.
         */
        public static MurmurHash3Values hash(SortedNumericDoubleValues values) {
            return new Double(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each long value.
         */
        public static MurmurHash3Values hash(SortedNumericDocValues values) {
            return new Long(values);
        }

        /**
         * Return a {@link MurmurHash3Values} instance that computes hashes on the fly for each binary value.
         */
        public static MurmurHash3Values hash(SortedBinaryDocValues values) {
            return new Bytes(values);
        }

        private static class Long extends MurmurHash3Values {

            private final SortedNumericDocValues values;

            Long(SortedNumericDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(values.nextValue());
            }
        }

        private static class Double extends MurmurHash3Values {

            private final SortedNumericDoubleValues values;

            Double(SortedNumericDoubleValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                return BitMixer.mix64(java.lang.Double.doubleToLongBits(values.nextValue()));
            }
        }

        private static class Bytes extends MurmurHash3Values {

            private final org.elasticsearch.common.hash.MurmurHash3.Hash128 hash = new org.elasticsearch.common.hash.MurmurHash3.Hash128();

            private final SortedBinaryDocValues values;

            Bytes(SortedBinaryDocValues values) {
                this.values = values;
            }

            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int count() {
                return values.docValueCount();
            }

            @Override
            public long nextValue() throws IOException {
                final BytesRef bytes = values.nextValue();
                org.elasticsearch.common.hash.MurmurHash3.hash128(bytes.bytes, bytes.offset, bytes.length, 0, hash);
                return hash.h1;
            }
        }
    }
}
