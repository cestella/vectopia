package com.caseystella.vectopia.frequency;

import com.caseystella.vectopia.cli.CLI;
import com.caseystella.vectopia.cli.UnigramConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;

import java.io.IOException;
import java.util.Set;

public class UnigramCLI extends CLI<UnigramConfig> {

  public UnigramCLI(String... argv) throws IOException {
    super(UnigramConfig.class, argv);
  }

  @Override
  public void run() {
    DataFrame df = NGramUtil.INSTANCE.open(config.getInputLoc(), config.getFilter(), sc);
    Broadcast<Set<String>> swBroadcast = sc.broadcast(stopwords);
    JavaPairRDD<String, Double> frequencies = NGramUtil.INSTANCE.frequencies(
              df
            , new UnigramMapper(swBroadcast)
            , new NGramUtil.InstanceFilter<>(config.getMinCount(), config.getMaxCount())
    );
    frequencies.cache();
    UnigramDB.INSTANCE.create(frequencies, config.getUnigramdb(), config.getMaxWords());
  }


  @Override
  protected String getName() {
    return "unigram";
  }

  public static void main(String... argv) throws Exception {
    UnigramCLI cli = new UnigramCLI(argv);
    cli.run();
  }
}
