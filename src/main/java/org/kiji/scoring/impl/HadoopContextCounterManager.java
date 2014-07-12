/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;

import org.kiji.scoring.CounterManager;

/**
 * CounterManager implementation which delegates to a Mapper.Context to store counter values.
 *
 * @param <INKEY> Input key type of the Hadoop Mapper from which this CounterManager gets its
 *     Context.
 * @param <INVALUE> Input value type of the Hadoop Mapper from which this CounterManager gets its
 *     Context.
 * @param <OUTKEY> Output key type of the Hadoop Mapper from which this CounterManager gets its
 *     Context.
 * @param <OUTVALUE> Output value type of the Hadoop Mapper from which this CounterManager gets its
 *     Context.
 */
public final class HadoopContextCounterManager<INKEY, INVALUE, OUTKEY, OUTVALUE>
    implements CounterManager {

  /**
   * Initialize a new HadoopContextCounterManager which delegates to the given Mapper.Context.
   *
   * @param hadoopContext Mapper.Context in which to store counters.
   * @param <INKEY> Input key type of the Hadoop Mapper from which this CounterManager gets its
   *     Context.
   * @param <INVALUE> Input value type of the Hadoop Mapper from which this CounterManager gets its
   *     Context.
   * @param <OUTKEY> Output key type of the Hadoop Mapper from which this CounterManager gets its
   *     Context.
   * @param <OUTVALUE> Output value type of the Hadoop Mapper from which this CounterManager gets
   *     its Context.
   * @return a new HadoopContextcounterManager.
   */
  public static <INKEY, INVALUE, OUTKEY, OUTVALUE> HadoopContextCounterManager create(
      final Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context hadoopContext
  ) {
    return new HadoopContextCounterManager(hadoopContext);
  }

  private final Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mHadoopContext;

  /**
   * Initialize a new HadoopContextCounterManager.
   *
   * @param hadoopContext Mapper.Context in which to store counters.
   */
  private HadoopContextCounterManager(
      final Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context hadoopContext
  ) {
    mHadoopContext = hadoopContext;
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final Enum<?> counter,
      final long value
  ) {
    mHadoopContext.getCounter(counter).increment(value);
  }

  /** {@inheritDoc} */
  @Override
  public void incrementCounter(
      final String group,
      final String name,
      final long value
  ) {
    mHadoopContext.getCounter(group, name).increment(value);
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final Enum<?> counter
  ) {
    return mHadoopContext.getCounter(counter).getValue();
  }

  /** {@inheritDoc} */
  @Override
  public Long getCounterValue(
      final String group,
      final String name
  ) {
    return mHadoopContext.getCounter(group, name).getValue();
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getGroups() {
    throw new UnsupportedOperationException(String.format(
        "%s does not support listing counter groups.", getClass().getName()));
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> getCountersInGroup(
      final String group
  ) {
    throw new UnsupportedOperationException(String.format(
        "%s does not support listing counters within a group.", getClass().getName()));
  }
}
