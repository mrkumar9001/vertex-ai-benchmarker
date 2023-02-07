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
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.aiplatform.v1beta1.EntityTypeName;
import com.google.cloud.aiplatform.v1beta1.FeaturestoreOnlineServingServiceClient;
import com.google.cloud.aiplatform.v1beta1.FeatureSelector;
import com.google.cloud.aiplatform.v1beta1.FeaturestoreOnlineServingServiceSettings;
import com.google.cloud.aiplatform.v1beta1.IdMatcher;
import com.google.cloud.aiplatform.v1beta1.ReadFeatureValuesRequest;
import com.google.cloud.aiplatform.v1beta1.ReadFeatureValuesResponse;
import com.google.cloud.aiplatform.v1beta1.StreamingReadFeatureValuesRequest;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

// Apply throughout, figure out FeatureStore (product?) vs Featurestore (an instance)
public class FeatureStoreApiCallerV1beta1 extends FeatureStoreApiCaller {
    private FeaturestoreOnlineServingServiceClient onlineClient;

    public FeatureStoreApiCallerV1beta1(
        String project, String location, FeatureStoreApiCaller.REST_METHOD method) {
        super(project, location, method);
        try {
            FeaturestoreOnlineServingServiceSettings onlineSettings = FeaturestoreOnlineServingServiceSettings.newBuilder(
                ).setEndpoint(endpoint).build();
            onlineClient = FeaturestoreOnlineServingServiceClient.create(onlineSettings);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }

    public FeatureStoreApiCallerV1beta1(
        String project, String location, String endpointOverride,
        FeatureStoreApiCaller.REST_METHOD method) {
        super(project, location, endpointOverride, method);
        try {
            FeaturestoreOnlineServingServiceSettings onlineSettings = FeaturestoreOnlineServingServiceSettings.newBuilder(
                ).setEndpoint(endpoint).build();
            onlineClient = FeaturestoreOnlineServingServiceClient.create(onlineSettings);
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }

    public FeatureStoreLoadTestResult call(FeatureStoreInput featureStoreInput) {
        if (featureStoreInput.getEntityIDs().size() > 0) {
            return streamingReadFeaturesValuesCall(featureStoreInput);
        } else if (featureStoreInput.getEntityID().isPresent()) {
            return readFeaturesValuesCall(featureStoreInput);
        } else {
            throw new IllegalArgumentException("Malformed FeatureStoreInput");
        }
    }

  private FeatureStoreLoadTestResult readFeaturesValuesCall(FeatureStoreInput featureStoreInput) {
    ReadFeatureValuesRequest request = ReadFeatureValuesRequest.newBuilder()
        .setEntityType(
            EntityTypeName.of(project, location, featureStoreInput.getFeatureStoreID(),
                    featureStoreInput.getEntityType())
                .toString())
        .setEntityId(featureStoreInput.getEntityID().get())
        .setFeatureSelector(FeatureSelector.newBuilder().setIdMatcher(
            IdMatcher.newBuilder().addAllIds(featureStoreInput.getFeatureIDs()).build()).build())
        .build();

    return callWrapper(() -> {
      ReadFeatureValuesResponse response = onlineClient.readFeatureValues(request);
      return response.getSerializedSize();
    });
  }

    private FeatureStoreLoadTestResult streamingReadFeaturesValuesCall(FeatureStoreInput featureStoreInput) {
        StreamingReadFeatureValuesRequest request = StreamingReadFeatureValuesRequest.newBuilder()
           .setEntityType(
               EntityTypeName.of(project, location, featureStoreInput.getFeatureStoreID(),
                                    featureStoreInput.getEntityType())
                   .toString())
           .addAllEntityIds(featureStoreInput.getEntityIDs())
           .setFeatureSelector(FeatureSelector.newBuilder().setIdMatcher(IdMatcher.newBuilder().addAllIds(featureStoreInput.getFeatureIDs()).build()).build())
           .build();

      return callWrapper(() -> {
        ServerStream<ReadFeatureValuesResponse> response = onlineClient.streamingReadFeatureValuesCallable()
            .call(request);
        int numEntities = 0;
        int responseSize = 0;
        Iterator<ReadFeatureValuesResponse> iter = response.iterator();
        while (iter.hasNext()) {
          ReadFeatureValuesResponse entityResponse = iter.next();
          numEntities++;
          responseSize += entityResponse.getSerializedSize();
        }
        return responseSize;
      });
    }
}