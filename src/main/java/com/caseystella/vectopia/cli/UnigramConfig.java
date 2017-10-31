package com.caseystella.vectopia.cli;

import com.caseystella.vectopia.cli.Config;

public class UnigramConfig extends Config {

  String unigramdb;
  Integer maxCount;
  Integer minCount;
  Integer maxWords;


  public String getUnigramdb() {
    return unigramdb;
  }

  public void setUnigramdb(String unigramdb) {
    this.unigramdb = unigramdb;
  }

  public Integer getMaxCount() {
    return maxCount;
  }

  public void setMaxCount(Integer maxCount) {
    this.maxCount = maxCount;
  }

  public Integer getMinCount() {
    return minCount;
  }

  public void setMinCount(Integer minCount) {
    this.minCount = minCount;
  }

  public Integer getMaxWords() {
    return maxWords;
  }

  public void setMaxWords(Integer maxWords) {
    this.maxWords = maxWords;
  }
}
