/*
 * Copyright (c) 2020 Cognite AS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognite.beam.io.transform;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/*
 * Breaks Dataflow fusion by doing GroupByKey/Ungroup that forces materialization of the data,
 * thus preventing Dataflow form fusing steps before and after this transform.
 * This is useful to insert in cases where a series of transforms deal with very small sets of data
 * that act as descriptors of very heavy workloads in subsequent steps (e.g. a collection of file names
 * where each file takes a long time to process).
 * In this case Dataflow might over-eagerly fuse steps dealing with small data sets with the "heavy"
 * processing steps, which will result in heavy steps being executed on a single worker.
 * Typical usage:
 *  ...
 *  PCollection<String> fileNames = pipeline.apply(...);
 *  fileNames.apply(new BreakFusionTransform<String>())
 *      .apply(new HeavyFileProcessingTransform())
 *      .....
 */
public class BreakFusion<T> extends PTransform<PCollection<T>, PCollection<T>> {

  public static <T> BreakFusion<T> create() {
    return new BreakFusion<T>();
  }

  private BreakFusion() {}

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply("Add random key", ParDo.of(new BreakFusion.AddRandomKeyFn<T>()))
        .apply("Group by key", GroupByKey.<Integer, T>create())
        .apply("Extract values", Values.<Iterable<T>>create())
        .apply("Flatten iterables", Flatten.iterables());
  }

  static class AddRandomKeyFn<T> extends DoFn<T, KV<Integer, T>> {
    @DoFn.ProcessElement
    public void processElement(@Element T element, OutputReceiver<KV<Integer, T>> out)
        throws Exception {
      out.output(KV.of(ThreadLocalRandom.current().nextInt(), element));
    }
  }
}
