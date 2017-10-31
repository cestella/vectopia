package com.caseystella.vectopia.frequency;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.mapdb.HTreeMap;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BigramMapper implements PairFlatMapFunction<Row, Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Integer> {

  private Broadcast<Set<String>> stopwords;
  private String dbLoc;
  private transient HTreeMap<String, UnigramDB.RankedFrequency> unigramCache;

  public BigramMapper(Broadcast<Set<String>> stopwords, String dbLoc) {
    this.stopwords = stopwords;
    this.dbLoc = dbLoc;
  }

  @Override
  public Iterable<Tuple2<Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Integer>> call(Row row) throws Exception {
    List<Tuple2<Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Integer>> ret = new ArrayList<>();
    String words = row.getString(0);
    Integer count = row.getInt(2);
    HTreeMap<String, UnigramDB.RankedFrequency> db = getUnigramCache();
    for(Tuple2<String, String> bigram : NLPUtil.INSTANCE.skipgrams(words, stopwords.getValue())) {
      UnigramDB.RankedFrequency w1 = db.get(bigram._1);
      UnigramDB.RankedFrequency w2 = db.get(bigram._2);
      if(w1 != null && w2 != null) {
        ret.add(new Tuple2<>(new Tuple2<>(w1, w2), count));
      }
    }
    return ret;
  }

  public synchronized HTreeMap<String, UnigramDB.RankedFrequency> getUnigramCache() {
      if(unigramCache == null) {
        unigramCache = UnigramDB.INSTANCE.openFrequencyCache(dbLoc);
      }
      return unigramCache;
    }
}
