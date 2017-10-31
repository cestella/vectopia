package com.caseystella.vectopia.vectorize;

import java.io.Serializable;

public interface PMIStrategy extends Serializable {
  double pmi(double p_x, double p_y, double p_xy);
}
