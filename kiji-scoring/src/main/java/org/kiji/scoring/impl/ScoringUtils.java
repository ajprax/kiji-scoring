/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.impl;

import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;

/** Utility class for commonly used methods in KijiScoring. */
@ApiAudience.Private
public final class ScoringUtils {

  /** Utility classes may not be instantiated. */
  private ScoringUtils() { }

  /**
   * Create a new instance of the named ScoreFunction subclass.
   *
   * @param scoreFunctionClassName fully qualified class name of the ScoreFunction subclass to
   *     instantiate.
   * @return a new instance of the named ScoreFunction subclass.
   */
  public static ScoreFunction scoreFunctionForName(
      final String scoreFunctionClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(scoreFunctionClassName).asSubclass(ScoreFunction.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Create a new instance of the named KijiFreshnessPolicy subclass.
   *
   * @param policyClassName fully qualified class name of the KijiFreshnessPolicy subclass to
   *     instantiate.
   * @return a new instance of the named KijiFreshnessPolicy subclass.
   */
  public static KijiFreshnessPolicy policyForName(
      final String policyClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policyClassName).asSubclass(KijiFreshnessPolicy.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }
}
