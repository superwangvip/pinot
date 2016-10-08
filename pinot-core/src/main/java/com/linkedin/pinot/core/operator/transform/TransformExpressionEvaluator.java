/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.request.transform.result.TransformResult;


/**
 * Interface for Transform expressions evaluation.
 */
public interface TransformExpressionEvaluator {

  /**
   * Initializations that need to be performed before {@link #evaluate(int[])}
   * can be called.
   */
  void init();

  /**
   * Evaluates the transform expression on a given set of docIds.
   * @param docIdSet Array of doc ids on which to evaluate the expression.
   * @param length Length of doc Ids to evaluate (array size could be larger).
   */
  void evaluate(int[] docIdSet, int length);

  /**
   * Post processing (if any) to be done after all doc ids have been precessed, and
   * before {@link #getResult()} can be called.
   */
  void finish();

  /**
   * Returns the results of transform expression evaluation.
   * @return Result of transform expression
   */
  TransformResult getResult();
}
