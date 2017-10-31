package com.caseystella.vectopia.frequency;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

public enum UnigramDB {
  INSTANCE;
  public static int NUM_THREADS = 5;
  public static String UNIGRAM_DB = "unigram";
  public static String WORD_INDEX = "word_index";

  public static class RankedFrequency implements Serializable {
    public static final Serializer<RankedFrequency> SERIALIZER = new Serializer<RankedFrequency>() {
      @Override
      public void serialize(@NotNull DataOutput2 out, @NotNull RankedFrequency rankedFrequency) throws IOException {
        out.writeDouble(rankedFrequency.getFrequency());
        out.writeLong(rankedFrequency.getRank());
      }

      @Override
      public RankedFrequency deserialize(@NotNull DataInput2 in, int i) throws IOException {
        double frequency = in.readDouble();
        long rank = in.readLong();
        return new RankedFrequency(frequency, rank);
      }
    };
    private final double frequency;
    private final long rank;
    public RankedFrequency(double frequency, long rank) {
      this.frequency = frequency;
      this.rank = rank;
    }

    public double getFrequency() {
      return frequency;
    }

    public long getRank() {
      return rank;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RankedFrequency that = (RankedFrequency) o;

      if (Double.compare(that.getFrequency(), getFrequency()) != 0) return false;
      return getRank() == that.getRank();

    }

    @Override
    public int hashCode() {
      int result;
      long temp;
      temp = Double.doubleToLongBits(getFrequency());
      result = (int) (temp ^ (temp >>> 32));
      result = 31 * result + (int) (getRank() ^ (getRank() >>> 32));
      return result;
    }
  }


  public HTreeMap<String, RankedFrequency> create(JavaPairRDD<String, Double> frequencies, String unigramDB, int maxVocab) {
    DB db = DBMaker.fileDB(unigramDB)
            .make();
    final HTreeMap<String, RankedFrequency> frequencyMap = db
            .hashMap(UNIGRAM_DB, Serializer.STRING, RankedFrequency.SERIALIZER)
            .create();
    final HTreeMap<Long, String> index = db
            .hashMap(WORD_INDEX, Serializer.LONG, Serializer.STRING)
            .create();
    frequencies.mapToPair(t -> new Tuple2<>(t._2, t._1)).sortByKey(false)
    .mapToPair( t -> new Tuple2<>(t._2, t._1));

    List<Tuple2<String, Double>> words = frequencies.take(maxVocab);

    final AtomicLong rank = new AtomicLong(0L);
    Iterable<List<Tuple2<String, Double>>> partitions = Iterables.partition(words, words.size() / NUM_THREADS);
    try {
      ForkJoinPool forkJoinPool = new ForkJoinPool(NUM_THREADS);
      forkJoinPool.submit(() -> partitions.forEach(p -> {
                for (Tuple2<String, Double> t : p) {
                  long i = rank.getAndIncrement();
                  frequencyMap.put(t._1, new RankedFrequency(t._2, i));
                  index.put(i, t._1);
                }
              }
        )
      );
      return frequencyMap;
    }
    finally {
      frequencyMap.close();
      index.close();
      db.close();
    }
  }

  public HTreeMap<Long, String> openWordIndex(String loc) {
    DB db = DBMaker.fileDB(loc)
            .readOnly()
            .closeOnJvmShutdown()
            .make();
    return db.hashMap(WORD_INDEX, Serializer.LONG, Serializer.STRING)
             .open();
  }

  public HTreeMap<String, RankedFrequency> openFrequencyCache(String loc) {
    DB db = DBMaker.fileDB(loc)
            .readOnly()
            .closeOnJvmShutdown()
            .make();
    return db.hashMap(UNIGRAM_DB, Serializer.STRING, RankedFrequency.SERIALIZER)
             .open();
  }

}
