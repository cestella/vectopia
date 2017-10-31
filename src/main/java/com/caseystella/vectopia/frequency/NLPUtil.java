package com.caseystella.vectopia.frequency;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public enum NLPUtil {
  INSTANCE;
  public Set<String> load(File stopwordList) throws IOException {
    Set<String> ret = new HashSet<>();
    if(stopwordList.exists()) {
      try (BufferedReader reader = Files.newBufferedReader(stopwordList.toPath())) {
        for (String line = null; (line = reader.readLine()) != null; ) {
          ret.add(line.trim());
        }
      }
    }
    return ret;
  }

  public boolean include(String word, Set<String> stopwordList) {
    return !stopwordList.contains(word);
  }

  public Optional<String> transform(String word) {
    if(StringUtils.isEmpty(word)) {
      return Optional.empty();
    }
    word = word.toLowerCase().trim();
    word = word.replaceAll("[^a-zA-Z]", "");
    return StringUtils.isEmpty(word)?Optional.empty():Optional.of(word);
  }

  public List<Tuple2<String, String>> skipgrams(String str, Set<String> stopwordList) {
    List<Tuple2<String, String>> ngrams = new ArrayList<>();
    List<String> words = new ArrayList<>();
    Iterable<String> splitWords = Splitter.on(" ").split(str);
    Iterable<Optional<String>> transformedWords = Iterables.transform(splitWords, w -> transform(w));
    Iterable<Optional<String>> filteredWords =
            Iterables.filter(transformedWords
                            , w -> w.isPresent() && !stopwordList.contains(w.get())
                            );
    Iterables.addAll(words, Iterables.transform(filteredWords, w -> w.get()));
    for(int i = 0;i < words.size();++i) {
      String w1 = words.get(i);
      for(int j = i+1;j < words.size();++j) {
        String w2 = words.get(j);
        ngrams.add(new Tuple2<>(w1, w2));
      }
    }
    return ngrams;
  }

}
