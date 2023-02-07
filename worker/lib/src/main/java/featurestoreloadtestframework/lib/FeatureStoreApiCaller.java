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
import java.time.Duration;
import java.time.Instant;

public abstract class FeatureStoreApiCaller {
	public enum REST_METHOD {
		GET,
		LIST
	}
	protected String project;
	protected String location;
	protected String endpoint;
	protected REST_METHOD method;  // Evaluate if this should live somewhere else

	public FeatureStoreApiCaller(String project, String location, REST_METHOD method) {
		this(project, location, String.format("%s-aiplatform.googleapis.com:443", location), method);
	}

	public FeatureStoreApiCaller(String project, String location, String endpointOverride,
		REST_METHOD method) {
		this.project = project;
		this.location = location;
		this.endpoint = endpointOverride;
		this.method = method;
	}

	public abstract FeatureStoreLoadTestResult call(FeatureStoreInput featureStoreInput);

	interface ReadRequest {
		/**
		 * Performs the read request code and returns response size.
		 * TODO: Return # entities read.
		 */
		int call() throws ApiException;
	}

	/**
	 * Helper method to return a FeatureStoreLoadTestResult.
	 * @param readRequest The function to call which will perform the request and return response size.
	 * @return The load test result.
	 */
	protected FeatureStoreLoadTestResult callWrapper(ReadRequest readRequest) {
		Instant startTime = Instant.now();
		try {
			int responseSize = readRequest.call();
			Duration latency = Duration.between(startTime, Instant.now());
			return FeatureStoreLoadTestResult.successfulRun(startTime, latency, responseSize);
		} catch (ApiException e) {
			Duration latency = Duration.between(startTime, Instant.now());
			return FeatureStoreLoadTestResult.failingRun(startTime, latency, e);
		}
	}

}