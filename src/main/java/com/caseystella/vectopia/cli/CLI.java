package com.caseystella.vectopia.cli;

import com.caseystella.vectopia.frequency.NLPUtil;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public abstract class CLI<T extends Config> {
  private static ThreadLocal<ObjectMapper> _mapper = ThreadLocal.withInitial(
          () -> new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
  );
  protected JavaSparkContext sc;
  protected T config;
  protected Set<String> stopwords;
  public CLI(Class<T> configClass, String... argv) throws IOException {
    createContext(getName());
    File configFile = new File(argv[0]);
    config = _mapper.get().readValue(configFile, configClass);
    this.stopwords = stopwords();
  }

  public abstract void run();
  protected abstract String getName();

  private Set<String> stopwords() throws IOException {
    Set<String> stopwords = new HashSet<>();
    for(String line : Files.readAllLines(new File(config.getStopwordsList()).toPath())) {
      Optional<String> word = NLPUtil.INSTANCE.transform(line.trim());
      if(word.isPresent()) {
        stopwords.add(word.get());
      }
    }
    return stopwords;
  }

  private JavaSparkContext createContext(String name) {
    SparkConf conf = new SparkConf().setAppName(name);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    return new JavaSparkContext(conf);
  }

}
