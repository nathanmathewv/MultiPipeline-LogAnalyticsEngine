package com.example.multietl.pipelines.mapreduce.jobs;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class LocalMapReduceJob<I, K, V, O> {
    public List<O> run(List<I> inputs) {
        Map<K, List<V>> grouped = newGroupingMap();
        for (I input : inputs) {
            map(input, (key, value) -> grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(value));
        }

        List<O> outputs = new ArrayList<>();
        for (Map.Entry<K, List<V>> entry : grouped.entrySet()) {
            reduce(entry.getKey(), entry.getValue(), outputs::add);
        }
        return outputs;
    }

    protected Map<K, List<V>> newGroupingMap() {
        return new LinkedHashMap<>();
    }

    protected abstract void map(I input, KeyValueCollector<K, V> collector);

    protected abstract void reduce(K key, List<V> values, OutputCollector<O> collector);

    @FunctionalInterface
    public interface KeyValueCollector<K, V> {
        void collect(K key, V value);
    }

    @FunctionalInterface
    public interface OutputCollector<O> {
        void collect(O output);
    }
}
