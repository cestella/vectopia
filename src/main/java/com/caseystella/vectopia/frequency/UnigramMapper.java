package com.caseystella.vectopia.frequency;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class UnigramMapper implements PairFlatMapFunction<Row, String, Integer> {
  Broadcast<Set<String>> stopwords;
  public UnigramMapper(Broadcast<Set<String>> stopwords) {
    this.stopwords = stopwords;
  }

  @Override
  public Iterable<Tuple2<String, Integer>> call(Row row) throws Exception {
    List<Tuple2<String, Integer>> ret = new ArrayList<>();
    Optional<String> word = NLPUtil.INSTANCE.transform(row.getString(0));
    if(word.isPresent() && NLPUtil.INSTANCE.include(word.get(), stopwords.getValue())) {
      Integer count = row.getInt(2);
      ret.add(new Tuple2<>(word.get(), count));
    }
    return ret;
  }
}
