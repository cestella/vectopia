package com.caseystella.vectopia.cli;

import com.caseystella.vectopia.vectorize.PMIStrategies;

public class VectorizeConfig extends Config {
  String frequencyDBLoc;
  int vocabSize;
  int dimension;
  String outputLoc;
  String inputLoc;
  PMIStrategies pmiStrategies = PMIStrategies.PMI;

  public PMIStrategies getPmiStrategy() {
    return pmiStrategies;
  }

  public void setPmiStrategy(PMIStrategies pmiStrategies) {
    this.pmiStrategies = pmiStrategies;
  }

  public String getInputLoc() {
    return inputLoc;
  }

  public void setInputLoc(String inputLoc) {
    this.inputLoc = inputLoc;
  }

  public String getFrequencyDBLoc() {
    return frequencyDBLoc;
  }

  public void setFrequencyDBLoc(String frequencyDBLoc) {
    this.frequencyDBLoc = frequencyDBLoc;
  }

  public int getVocabSize() {
    return vocabSize;
  }

  public void setVocabSize(int vocabSize) {
    this.vocabSize = vocabSize;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public String getOutputLoc() {
    return outputLoc;
  }

  public void setOutputLoc(String outputLoc) {
    this.outputLoc = outputLoc;
  }
}
