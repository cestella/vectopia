package com.caseystella.vectopia.vectorize;

import com.caseystella.vectopia.frequency.UnigramDB;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.mapdb.HTreeMap;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public enum MatrixUtil {
  INSTANCE;

  public static SparseVector combineSparseVectors(SparseVector... svs) {
    int size = 0;
    int nonzeros = 0;
    for (SparseVector sv : svs) {
      size += sv.size();
      nonzeros += sv.indices().length;
    }

    if (nonzeros != 0) {
      int[] indices = new int[nonzeros];
      double[] values = new double[nonzeros];

      int pointer_D = 0;
      int totalPt_D = 0;
      int pointer_V = 0;
      for (SparseVector sv : svs) {
        int[] indicesSV = sv.indices();
        for (int i : indicesSV) {
          indices[pointer_D++] = i + totalPt_D;
        }
        totalPt_D += sv.size();

        double[] valuesSV = sv.values();
        for (double d : valuesSV) {
          values[pointer_V++] = d;
        }

      }
      return new SparseVector(size, indices, values);
    } else {
      return null;
    }

  }

  public IndexedRowMatrix createMatrix(JavaPairRDD<Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Double> frequencies
                               , int vocSize
                               , PMIStrategy pmiStrategy
                               )
  {

    JavaPairRDD<Long, IndexedRow> rdd = frequencies.flatMapToPair(new ComputePMI(pmiStrategy, vocSize));
    JavaRDD<IndexedRow> rows = rdd.reduceByKey(
            (r1, r2) -> new IndexedRow( r1.index()
                                      , combineSparseVectors(r1.vector().toSparse(), r2.vector().toSparse())))
                                  .values();
    return new IndexedRowMatrix(rows.rdd());
  }


  public IndexedRowMatrix svd(IndexedRowMatrix matrix, int dimension) {
    SingularValueDecomposition<IndexedRowMatrix, Matrix> svd = matrix.computeSVD(dimension,true, 1.0E-9d);
    return svd.U();
  }


  public void persist(IndexedRowMatrix vectors
                     , String dbLoc
                     , String outFile
                     )
  {
    vectors.rows()
           .toJavaRDD()
           .map(new ToWordVecs(dbLoc))
           .saveAsTextFile(outFile)
           ;
  }

  public static class ToWordVecs implements Function<IndexedRow, String> {
    private transient HTreeMap<Long, String> wordIndex;
    private String dbLoc;

    public ToWordVecs(String dbLoc) {
      this.dbLoc= dbLoc;
    }

    @Override
    public String call(IndexedRow indexedRow) throws Exception {
      HTreeMap<Long, String> wordIndex = getWordIndex();
      String word = wordIndex.get(indexedRow.index());
      StringBuffer ret = new StringBuffer(word + ",");
      Vector vec = indexedRow.vector();
      for(int i = 0;i < vec.size()-1;++i) {
        ret.append(vec.apply(i) + ",");
      }
      ret.append(vec.apply(vec.size() - 1) );
      return ret.toString();
    }

    public synchronized HTreeMap<Long, String> getWordIndex() {
      if(wordIndex == null) {
        wordIndex = UnigramDB.INSTANCE.openWordIndex(dbLoc);
      }
      return wordIndex;
    }
  }

  public static class ComputePMI implements PairFlatMapFunction<Tuple2<Tuple2<UnigramDB.RankedFrequency,UnigramDB.RankedFrequency>,Double>, Long, IndexedRow> {
    private PMIStrategy pmiStrategy;
    private int vocSize;
    public ComputePMI( PMIStrategy strategy, int vocSize) {
      this.pmiStrategy = strategy;
      this.vocSize = vocSize;
    }

    @Override
    public Iterable<Tuple2<Long, IndexedRow>>
    call(Tuple2<Tuple2<UnigramDB.RankedFrequency, UnigramDB.RankedFrequency>, Double> t) throws Exception {
      List<Tuple2<Long, IndexedRow>> ret = new ArrayList<>();
      double p_xy = t._2;
      UnigramDB.RankedFrequency w1 = t._1._1;
      UnigramDB.RankedFrequency w2 = t._1._2;
      if(w1 == null || w2 == null) {
        return ret;
      }
      double pmi = pmiStrategy.pmi(w1.getFrequency(), w2.getFrequency(), p_xy);
      {
        SparseVector vector =  new SparseVector(vocSize, new int[] { (int)w2.getRank()}, new double[] { pmi });
        IndexedRow row = new IndexedRow(w1.getRank(), vector);
        ret.add(new Tuple2<>(w1.getRank(), row));
      }
      {
        SparseVector vector =  new SparseVector(vocSize, new int[] { (int)w1.getRank()}, new double[] { pmi });
        IndexedRow row = new IndexedRow(w1.getRank(), vector);
        ret.add(new Tuple2<>(w2.getRank(), row));
      }

      return ret;
    }

  }
}
