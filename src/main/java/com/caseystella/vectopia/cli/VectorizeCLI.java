package com.caseystella.vectopia.cli;

import com.caseystella.vectopia.frequency.BigramMapper;
import com.caseystella.vectopia.frequency.NGramUtil;
import com.caseystella.vectopia.frequency.UnigramDB;
import com.caseystella.vectopia.vectorize.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;

import java.io.IOException;
import java.util.Set;

public class VectorizeCLI extends CLI<VectorizeConfig> {
  public VectorizeCLI(Class<VectorizeConfig> configClass, String... argv) throws IOException {
    super(configClass, argv);
  }

  @Override
  public void run() {
    DataFrame df = NGramUtil.INSTANCE.open(config.getInputLoc(), config.getFilter(), sc);
    Broadcast<Set<String>> swBroadcast = sc.broadcast(stopwords);
    JavaPairRDD<Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Double> frequencies = NGramUtil.INSTANCE.frequencies(
              df
            , new BigramMapper(swBroadcast,config.getFrequencyDBLoc())
            , new NGramUtil.InstanceFilter<>()
    );
    frequencies.cache();
    IndexedRowMatrix matrix = MatrixUtil.INSTANCE.createMatrix(frequencies, config.getVocabSize(), config.getPmiStrategy());
    IndexedRowMatrix wordVectors = MatrixUtil.INSTANCE.svd(matrix, config.getDimension());
    MatrixUtil.INSTANCE.persist(wordVectors, config.getFrequencyDBLoc(), config.getOutputLoc());
  }

  @Override
  protected String getName() {
    return "vectorize";
  }
}
