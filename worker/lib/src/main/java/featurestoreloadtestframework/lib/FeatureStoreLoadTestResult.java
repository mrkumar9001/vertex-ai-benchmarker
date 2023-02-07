/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package featurestoreloadtestframework.lib;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ErrorDetails;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;
import java.util.Optional;

public class FeatureStoreLoadTestResult {

  private final Instant startTime;
  private final Duration latency;

  private final boolean success;
  private final Optional<Code> errorCode;
  final Optional<ErrorDetails> errorDetails;
  private final int responseSize;

  private FeatureStoreLoadTestResult(Instant startTime, Duration latency, boolean success,
      Optional<StatusCode.Code> statusCode, Optional<ErrorDetails> errorDetails, int responseSize) {
    this.startTime = startTime;
    this.latency = latency;
    this.success = success;
    this.errorCode = statusCode;
    this.errorDetails = errorDetails;
    this.responseSize = responseSize;
  }

  public static FeatureStoreLoadTestResult successfulRun(Instant startTime, Duration latency,
      int responseSize) {
    return new FeatureStoreLoadTestResult(startTime, latency, true, Optional.empty(),
        Optional.empty(), responseSize);
  }

  public static FeatureStoreLoadTestResult failingRun(Instant startTime, Duration latency,
      ApiException e) {
    ErrorDetails errorDetails = e.getErrorDetails();
    return new FeatureStoreLoadTestResult(startTime, latency, false,
        Optional.ofNullable(e.getStatusCode().getCode()), Optional.of(errorDetails), 0);
  }

  public String toString() {
    String dateTimeFormatter = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateTimeFormatter)
        .withZone(ZoneOffset.UTC);
    String startTime = dateFormatter.format(this.startTime);
    double latency = DurationUtils.toMillis(this.latency);

    String errorCodeStr = getErrorCode().isPresent() ? getErrorCode().get().toString() : "";
    return startTime + "," + latency + "," + this.success + "," + errorCodeStr + ","
        + "errorDetailsNotImplemented" + "," + this.responseSize;
  }

  public Instant getStartTime() {
    return this.startTime;
  }

  public Duration getLatency() {
    return this.latency;
  }

  /**
   * @return If the request was a success.
   */
  public boolean getSuccess() {
    return this.success;
  }

  /**
   * @return The error code. Only present if the request failed.
   */
  public Optional<Code> getErrorCode() {
    return this.errorCode;
  }
}
