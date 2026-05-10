package com.example.multietl.pipelines.base;

import java.util.EnumSet;
import java.util.Set;

public class QueryPlan {
    private final EnumSet<QueryType> queries;
    private final boolean splitByMonth;

    public QueryPlan(Set<QueryType> queries, boolean splitByMonth) {
        if (queries == null || queries.isEmpty()) {
            throw new IllegalArgumentException("queries must not be empty");
        }
        this.queries = EnumSet.copyOf(queries);
        this.splitByMonth = splitByMonth;
    }

    public static QueryPlan all(boolean splitByMonth) {
        return new QueryPlan(EnumSet.allOf(QueryType.class), splitByMonth);
    }

    public static QueryPlan single(QueryType queryType, boolean splitByMonth) {
        return new QueryPlan(EnumSet.of(queryType), splitByMonth);
    }

    public EnumSet<QueryType> getQueries() {
        return queries;
    }

    public boolean isSplitByMonth() {
        return splitByMonth;
    }
}
