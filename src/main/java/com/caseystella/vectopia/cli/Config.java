package com.caseystella.vectopia.cli;

public class Config {
  String stopwordsList;
  String filter;
  String inputLoc;

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }

  public String getInputLoc() {
    return inputLoc;
  }

  public void setInputLoc(String inputLoc) {
    this.inputLoc = inputLoc;
  }

  public String getStopwordsList() {
    return stopwordsList;
  }

  public void setStopwordsList(String stopwordsList) {
    this.stopwordsList = stopwordsList;
  }
}
