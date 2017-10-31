package com.caseystella.vectopia.nlp;

import com.caseystella.vectopia.BaseIntegrationTest;
import com.caseystella.vectopia.frequency.NGramUtil;
import com.caseystella.vectopia.frequency.UnigramMapper;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UnigramTest extends BaseIntegrationTest {


  /**
the 1991 1 1 1
the 1992 1 1 1
Casey 1991 2 1 1
Casey 1992 2 1 1
Casey 1892 2 1 1
cnn 1991 2 1 1
   */
  @Multiline
  public static String data;

  @Test
  public void testWithoutFilter() throws IOException {
    Set<String> stopwords = new HashSet<>();
    {
      stopwords.add("the");
    }
    File inFile = createFile(data.replace(" ", "\t"), getClass());
    DataFrame df = NGramUtil.INSTANCE.open(inFile.getAbsolutePath(), "year > 1990", sc);
    Broadcast<Set<String>> swBroadcast = sc.broadcast(stopwords);
    JavaPairRDD<String, Double> counts =
            NGramUtil.INSTANCE.frequencies(df, new UnigramMapper(swBroadcast), new NGramUtil.InstanceFilter<>());
    Map<String, Double> countsAsMap = counts.collectAsMap();
    Assert.assertEquals(2,countsAsMap.size());
    Assert.assertEquals(4.0/6.0, countsAsMap.get("casey"), 1e-5);
  }

  @Test
  public void testWithFilter() throws IOException {
    Set<String> stopwords = new HashSet<>();
    {
      stopwords.add("the");
    }
    File inFile = createFile(data.replace(" ", "\t"), getClass());
    DataFrame df = NGramUtil.INSTANCE.open(inFile.getAbsolutePath(), "year > 1990", sc);
    Broadcast<Set<String>> swBroadcast = sc.broadcast(stopwords);
    JavaPairRDD<String, Double> counts = NGramUtil.INSTANCE.frequencies(df, new UnigramMapper(swBroadcast), new NGramUtil.InstanceFilter<>(5, null));
    Map<String, Double> countsAsMap = counts.collectAsMap();
    Assert.assertEquals(0,countsAsMap.size());
  }
}
