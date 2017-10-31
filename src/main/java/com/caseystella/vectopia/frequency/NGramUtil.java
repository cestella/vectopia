package com.caseystella.vectopia.frequency;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

public enum NGramUtil {
  INSTANCE;

  private static StructType schema() {
    StructField[] fields = new StructField[5];
    fields[0] = new StructField("ngram", DataTypes.StringType, true, Metadata.empty());
    fields[1] = new StructField("year", DataTypes.IntegerType, true, Metadata.empty());
    fields[2] = new StructField("occurrences", DataTypes.IntegerType, true, Metadata.empty());
    fields[3] = new StructField("pages", DataTypes.IntegerType, true, Metadata.empty());
    fields[4] = new StructField("books", DataTypes.IntegerType, true, Metadata.empty());
    return new StructType(fields);
  }

  public DataFrame open(String input, String where, JavaSparkContext sc) {

    SQLContext sqlContext = new SQLContext(sc);
    DataFrameReader reader = sqlContext.read()
                                       .format("com.databricks.spark.csv")
                                       .option("delimiter", "\t")
                                       .option("header", "false")
                                       .option("inferSchema", "false")
                                       .schema(schema())
                                       ;
    DataFrame df = reader.load(input);
    if(where != null) {
      String tableName = Iterables.getFirst(Splitter.on('.').split(Iterables.getLast(Splitter.on('/').split(input), input)), input);
      System.out.println("Registering " + tableName + "...");
      df.registerTempTable(tableName);
      String query = "select * from " + tableName + " where " + where;
      return df.sqlContext().sql(query);
    }
    return df;
  }

  public <T> JavaPairRDD<T, Double>
  frequencies( DataFrame df
       , PairFlatMapFunction<Row, T, Integer> splitter
       , InstanceFilter<T> filter
       )
  {
    JavaPairRDD<T, Integer> count = df.javaRDD().flatMapToPair(splitter)
                       .reduceByKey(new CountReducer())
            ;
    count = count.cache();
    final long num =
    count.mapPartitions((FlatMapFunction<Iterator<Tuple2<T, Integer>>, Long>) it -> {
      long sum = 0;
      while(it.hasNext()) {
        sum += it.next()._2;
      }
      ArrayList ret1 = new ArrayList<>();
      ret1.add(sum);
      return ret1;
    }).reduce((l, r) -> l + r).longValue();

    JavaPairRDD<T, Integer> filteredCount = count.filter(filter);
    JavaPairRDD<T, Double> ret = filteredCount
                                      .flatMapToPair( t -> ImmutableList.of(new Tuple2<>(t._1, (1.0*t._2)/num)));
    return ret;
  }


  public static class InstanceFilter<T> implements org.apache.spark.api.java.function.Function<Tuple2<T, Integer>, Boolean> {


    int minimumCount;
    int maximumCount;
    public InstanceFilter() {
      this(null, null);
    }

    public InstanceFilter(Integer minimumCount, Integer maximumCount) {
      this.minimumCount = minimumCount == null?0:minimumCount;
      this.maximumCount = maximumCount == null?Integer.MAX_VALUE:maximumCount;
    }


    @Override
    public Boolean call(Tuple2<T, Integer> t) throws Exception {
      if(t._2 == null) {
        return false;
      }
      return t._2 > minimumCount && t._2 < maximumCount;
    }
  }

  public static class CountReducer implements Function2<Integer, Integer, Integer>  {

    @Override
    public Integer call(Integer integer, Integer integer2) throws Exception {
      return (integer == null?0:integer) + (integer2 == null?0:integer2);
    }
  }

}
