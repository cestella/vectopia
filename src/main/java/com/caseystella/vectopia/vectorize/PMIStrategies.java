package com.caseystella.vectopia.vectorize;

public enum PMIStrategies implements PMIStrategy{
  PMI( (p_x, p_y, p_xy) -> Math.log(p_xy/p_x/p_y))
  ;

  PMIStrategy strategy;
  PMIStrategies(PMIStrategy strategy) {
    this.strategy = strategy;
  }
  @Override
  public double pmi(double p_x, double p_y, double p_xy) {
    return strategy.pmi(p_x, p_y, p_xy);
  }
}
