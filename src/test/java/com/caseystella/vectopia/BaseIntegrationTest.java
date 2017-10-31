package com.caseystella.vectopia;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class BaseIntegrationTest {
  protected transient JavaSparkContext sc;
  protected transient SQLContext sqlContext;

  @Before
  public void setup() {
    SparkConf conf  = new SparkConf();
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sc = new JavaSparkContext("local", "JavaAPISuite", conf);
    sqlContext = new SQLContext(sc);
  }

  public <T> DataFrame createDf(List<T> data, Class<T> clazz) {
    JavaRDD<T> rdd = sc.parallelize(data);
    return sqlContext.createDataFrame(rdd, clazz);
  }

  public static File createFile(String data, Class clazz) throws IOException {
    File tmpFile = File.createTempFile(clazz.getName(), "csv");
    Files.write(tmpFile.toPath(), data.getBytes(), StandardOpenOption.CREATE);
    tmpFile.deleteOnExit();
    return tmpFile;
  }

  public static <T> DataFrame createDf(Class clazz, SQLContext context, String csvWithHeader) throws IOException {
    File tmpFile = createFile( csvWithHeader, clazz);
    try(PrintWriter pw = new PrintWriter(tmpFile)) {
      IOUtils.write(csvWithHeader, pw);
      pw.flush();
    }
    DataFrameReader reader = context.read()
                                       .format("com.databricks.spark.csv")
                                       .option("header", "true")
                                       .option("inferSchema", "true")
                                       ;
    return reader.load(tmpFile.getPath());
  }

  @After
  public void shutdown() {
    sc.stop();
  }
}
