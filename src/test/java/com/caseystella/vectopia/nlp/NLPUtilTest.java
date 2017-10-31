package com.caseystella.vectopia.nlp;

import com.caseystella.vectopia.frequency.NLPUtil;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NLPUtilTest {

  @Test
  public void testBigrams() {
    String sentence = "I hope that you have a good time";
    Set<String> stopwords = new HashSet<String>() {{
      add("i");
      add("a");
      add("you");
    }};
    List<Tuple2<String, String>> bigrams = NLPUtil.INSTANCE.skipgrams(sentence, stopwords);
    System.out.println(bigrams);
    int numWords = 8;
    int numWordsStopped = 3;
    int numWordsNotStopped = numWords - numWordsStopped;
    int numPairs = numWordsNotStopped*(numWordsNotStopped - 1);
    Assert.assertEquals(numPairs/2, bigrams.size());
    Assert.assertEquals(new Tuple2<>("hope","that"), bigrams.get(0));
    Assert.assertEquals(new Tuple2<>("hope","have"), bigrams.get(1));
    Assert.assertEquals(new Tuple2<>("hope","good"), bigrams.get(2));
    Assert.assertEquals(new Tuple2<>("hope","time"), bigrams.get(3));
  }

}
