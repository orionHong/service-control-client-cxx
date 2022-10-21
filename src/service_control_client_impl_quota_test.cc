/* Copyright 2017 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include <thread>
#include <chrono>

#include "src/mock_transport.h"

namespace google {
namespace service_control_client {

namespace {

const char kServiceName[] = "library.googleapis.com";
const char kServiceConfigId[] = "2016-09-19r0";

const char kRequest1[] = R"(
service_name: "library.googleapis.com"
allocate_operation {
  operation_id: "operation-1"
  method_name: "methodname"
  consumer_id: "consumerid"
  quota_metrics {
    metric_name: "metric_first"
    metric_values {
      int64_value: 1
    }
  }
  quota_metrics {
    metric_name: "metric_second"
    metric_values {
      int64_value: 1
    }
  }
  quota_mode: BEST_EFFORT
}
service_config_id: "2016-09-19r0"
)";

const char kSuccessResponse1[] = R"(
operation_id: "operation-1"
quota_metrics {
  metric_name: "serviceruntime.googleapis.com/api/consumer/quota_used_count"
  metric_values {
    labels {
      key: "/quota_name"
      value: "metric_first"
    }
    int64_value: 1
  }
  metric_values {
    labels {
      key: "/quota_name"
      value: "metric_first"
    }
    int64_value: 1
  }
}
service_config_id: "2016-09-19r0"
)";

const char kErrorResponse1[] = R"(
operation_id: "operation-1"
allocate_errors {
  code: RESOURCE_EXHAUSTED
  subject: "user:integration_test_user"
}
)";

}  // namespace

class ServiceControlClientImplQuotaTest : public testing::Test {
 public:
  void SetUp() {
    ASSERT_TRUE(TextFormat::ParseFromString(kRequest1, &quota_request1_));
    ASSERT_TRUE(
        TextFormat::ParseFromString(kSuccessResponse1, &pass_quota_response1_));
    ASSERT_TRUE(
        TextFormat::ParseFromString(kErrorResponse1, &error_quota_response1_));

    // Initialize the client instance with cache enabled
    ServiceControlClientOptions cached_options(
        CheckAggregationOptions(10 /*entries */, 500 /* refresh_interval_ms */,
                                1000 /* expiration_ms */),
        QuotaAggregationOptions(10 /*entries */, 500 /* refresh_interval_ms */),
        ReportAggregationOptions(10 /* entries */, 500 /*flush_interval_ms*/));

    cached_options.quota_transport = mock_quota_transport_.GetFunc();

    cached_client_ = CreateServiceControlClient(kServiceName, kServiceConfigId,
                                                cached_options);

    // Initialize the client instance with cache disabled
    ServiceControlClientOptions noncached_options(
        CheckAggregationOptions(0, 500, 1000), QuotaAggregationOptions(0, 500),
        ReportAggregationOptions(0, 500));

    noncached_options.quota_transport = mock_quota_transport_.GetFunc();

    noncached_client_ = CreateServiceControlClient(
        kServiceName, kServiceConfigId, noncached_options);
  }

  AllocateQuotaRequest quota_request1_;
  AllocateQuotaResponse pass_quota_response1_;
  AllocateQuotaResponse error_quota_response1_;

  // Store some commonly used variables as member variables to prevent
  // use-after-destruction in tests.
  AllocateQuotaResponse quota_response_;

  // Clients must be the second to be destructed.
  // During destruction, the internal caches will be flushed and results will
  // be written to the variables above.
  std::unique_ptr<ServiceControlClient> cached_client_;
  std::unique_ptr<ServiceControlClient> noncached_client_;

  // Transports must be first to be destructed.
  // During destruction, threads will complete and results will be written to
  // the caches above.
  MockQuotaTransport mock_quota_transport_;
};

// Error on different service name
TEST_F(ServiceControlClientImplQuotaTest, TestQuotaWithInvalidServiceName) {
  ServiceControlClientOptions options(CheckAggregationOptions(10, 500, 1000),
                                      QuotaAggregationOptions(10, 500),
                                      ReportAggregationOptions(10, 500));

  options.quota_transport = mock_quota_transport_.GetFunc();

  std::unique_ptr<ServiceControlClient> client =
      CreateServiceControlClient("unknown", kServiceConfigId, options);

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  client->Quota(quota_request1_, &quota_response,
                [&done_status](Status status) { done_status = status; });

  EXPECT_EQ(
      done_status,
      Status(
          StatusCode::kInvalidArgument,
          "Invalid service name: library.googleapis.com Expecting: unknown"));

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 0);

  Statistics stat;
  Status stat_status = client->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 1);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 0);
}

// Error on default callback is not assigned
TEST_F(ServiceControlClientImplQuotaTest,
       TestQuotaWithNoDefaultQuotaTransport) {
  ServiceControlClientOptions options(CheckAggregationOptions(10, 500, 1000),
                                      QuotaAggregationOptions(10, 500),
                                      ReportAggregationOptions(10, 500));

  options.quota_transport = nullptr;

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;
  std::unique_ptr<ServiceControlClient> client =
      CreateServiceControlClient("unknown", kServiceConfigId, options);

  client->Quota(quota_request1_, &quota_response,
                [&done_status](Status status) { done_status = status; });

  EXPECT_EQ(done_status,
            Status(StatusCode::kInvalidArgument, "transport is NULL."));

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 0);

  Statistics stat;
  Status stat_status = client->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 1);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 0);
}

// Cached: false, Callback: stored
TEST_F(ServiceControlClientImplQuotaTest,
       TestNonCachedQuotaWithStoredCallbackMultipleReqeusts) {
  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillRepeatedly(
          Invoke(&mock_quota_transport_,
                 &MockQuotaTransport::AllocateQuotaWithStoredCallback));

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  // call Quota 10 times
  for (int i = 0; i < 10; i++) {
    noncached_client_->Quota(
        quota_request1_, &quota_response,
        [&done_status](Status status) { done_status = status; });
    EXPECT_EQ(done_status, UnknownError(""));
  }

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 10);

  // execute stored callback
  for (auto callback : mock_quota_transport_.on_done_vector_) {
    done_status = UnknownError("");
    callback(OkStatus());
    EXPECT_EQ(done_status, OkStatus());
  }

  Statistics stat;
  Status stat_status = noncached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 10);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 10);
}

TEST_F(ServiceControlClientImplQuotaTest, TestCachedQuotaRefreshGotHTTPError) {
  // Set callback function to return the negative response
  mock_quota_transport_.quota_response_ = &error_quota_response1_;
  mock_quota_transport_.done_status_ = OkStatus();

  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillRepeatedly(
          Invoke(&mock_quota_transport_,
                 &MockQuotaTransport::AllocateQuotaWithInplaceCallback));

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  // Triggers the initial AllocateQuotaRequset and insert a temporary positive
  // response to quota cache.
  cached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });
  EXPECT_EQ(done_status, OkStatus());
  // Check quota_response is positive
  EXPECT_EQ(quota_response.allocate_errors_size(), 0);

  // AllocateQuotaFlushCallback replaces the cached response with the
  // negative response, QUOTA_EXHAUSTED

  // Set the callback function to return a fail-close negative status, e.g.
  // CancelledError, to simulate HTTP error. A fail-close negative status
  // will be cached as a negative response.
  Status cancelledError = CancelledError("cancelled error");
  mock_quota_transport_.done_status_ = cancelledError;

  // Wait 600ms to let the cached response expire.
  // This sleep will not trigger a remote quota call since test did not provide
  // a timer object. The refresh remote quota call is triggered by cache lookup
  // in the "Quota" call, for an expired entry, if it has aggregated cost or it
  // is negative, make a remote quota call.
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  // Next Quota call reads the cached negative response, QUOTA_EXHAUSTED, and
  // triggers the quota cache refresh. The CancelledError response will be
  // cached after this.
  cached_client_->Quota(quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });
  // Check the cached response is the first cached response - QUOTA_EXHAUSTED.
  EXPECT_EQ(quota_response.allocate_errors_size(), 1);
  EXPECT_EQ(quota_response.allocate_errors()[0].code(),
            error_quota_response1_.allocate_errors()[0].code());

  // AllocateQuotaFlushCallback replaces the cached negative response with
  // the newly cached negative response - CancelledError.

  // Set the callback function to return a fail-open negative status, e.g.
  // InternalError. A fail-open dummy response will be cached as a positive
  // response.
  mock_quota_transport_.done_status_ = InternalError("internal error");

  // Wait 600ms to let the cached response expire.
  std::this_thread::sleep_for(std::chrono::milliseconds(600));

  // Read the previously cached CancelledError response, since it is a negative
  // response, it will trigger a new remote quota call. The new fail-open InternalError
  // response will be cached right after.
  cached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });

  // Check the returned response is the previously cached CancelledError response.
  // A fail-close error response is cached as:
  // AllocateQuoteResponse{allocate_errors:[QuotaError{code:UNSPECIFIED,status:error_status}]}
  EXPECT_EQ(quota_response.allocate_errors_size(), 1);
  EXPECT_EQ(quota_response.allocate_errors()[0].status().code(),
            (int)cancelledError.code());
  EXPECT_EQ(quota_response.allocate_errors()[0].status().message(),
            cancelledError.message());

  // Read the newly cached positive dummy response for fail-open.
  // The dummy response contains no allocate errors.
  cached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });
  EXPECT_EQ(quota_response.allocate_errors_size(), 0);

  Statistics stat;
  Status stat_status = cached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 4);
  EXPECT_EQ(stat.send_quotas_by_flush, 3);
  EXPECT_EQ(stat.send_quotas_in_flight, 0);
}

// Cached: true, Callback: stored
TEST_F(ServiceControlClientImplQuotaTest, TestCachedQuotaWithStoredCallback) {
  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillOnce(Invoke(&mock_quota_transport_,
                       &MockQuotaTransport::AllocateQuotaWithStoredCallback));

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  cached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });
  EXPECT_EQ(done_status, OkStatus());

  // call Quota 10 times
  for (int i = 0; i < 10; i++) {
    Status cached_done_status = UnknownError("");
    AllocateQuotaResponse cached_quota_response;
    cached_client_->Quota(
        quota_request1_, &cached_quota_response,
        [&cached_done_status](Status status) { cached_done_status = status; });
    EXPECT_EQ(cached_done_status, OkStatus());
  }

  // count stored callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 1);

  // execute stored callback
  mock_quota_transport_.on_done_vector_[0](OkStatus());
  EXPECT_EQ(done_status, OkStatus());

  Statistics stat;
  Status stat_status = cached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 11);
  EXPECT_EQ(stat.send_quotas_by_flush, 1);
  EXPECT_EQ(stat.send_quotas_in_flight, 0);
}

// Cached: false, Callback: in place
TEST_F(ServiceControlClientImplQuotaTest,
       TestNonCachedQuotaWithInPlaceCallbackMultipleReqeusts) {
  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillRepeatedly(
          Invoke(&mock_quota_transport_,
                 &MockQuotaTransport::AllocateQuotaWithInplaceCallback));

  // call Quota 10 times
  for (int i = 0; i < 10; i++) {
    AllocateQuotaResponse quota_response;
    Status done_status = UnknownError("");
    noncached_client_->Quota(
        quota_request1_, &quota_response,
        [&done_status](Status status) { done_status = status; });
    EXPECT_EQ(done_status, OkStatus());
  }

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 0);

  Statistics stat;
  Status stat_status = noncached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 10);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 10);
}

// Cached: true, Callback: in place
TEST_F(ServiceControlClientImplQuotaTest, TestCachedQuotaWithInPlaceCallback) {
  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillRepeatedly(
          Invoke(&mock_quota_transport_,
                 &MockQuotaTransport::AllocateQuotaWithInplaceCallback));

  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  cached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });

  EXPECT_EQ(done_status, OkStatus());

  // call Quota 10 times
  for (int i = 0; i < 10; i++) {
    cached_client_->Quota(
        quota_request1_, &quota_response,
        [&done_status](Status status) { done_status = status; });
    EXPECT_EQ(done_status, OkStatus());
  }

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 0);

  Statistics stat;
  Status stat_status = cached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 11);
  EXPECT_EQ(stat.send_quotas_by_flush, 1);
  EXPECT_EQ(stat.send_quotas_in_flight, 0);
}

// Cached: false, Callback: local in place
TEST_F(ServiceControlClientImplQuotaTest,
       TestNonCachedQuotaWithLocalInPlaceCallback) {
  Status done_status = UnknownError("");
  AllocateQuotaResponse quota_response;

  int callbackExecuteCount = 0;

  noncached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; },
      [&callbackExecuteCount](const AllocateQuotaRequest& request,
                                    AllocateQuotaResponse* response,
                                    TransportDoneFunc on_done) {
        callbackExecuteCount++;
        on_done(OkStatus());
      });

  EXPECT_EQ(done_status, OkStatus());

  EXPECT_EQ(callbackExecuteCount, 1);

  // count store callback
  EXPECT_EQ(mock_quota_transport_.on_done_vector_.size(), 0);

  Statistics stat;
  Status stat_status = noncached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 1);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 1);
}

// Cached: false, Callback: in place
TEST_F(ServiceControlClientImplQuotaTest,
       TestNonCachedQuotaWithInplaceCallbackNetworkErrorFailOpen) {
  // Set the callback function to return a gRPC error. When there's a gRPC
  // error, we consider it a fail-open network error.
  mock_quota_transport_.done_status_ = CancelledError("cancelled error");

  EXPECT_CALL(mock_quota_transport_, Quota(_, _, _))
      .WillOnce(
          Invoke(&mock_quota_transport_,
                 &MockQuotaTransport::AllocateQuotaWithInplaceCallback));

  Status done_status = OkStatus();
  AllocateQuotaResponse quota_response;
  noncached_client_->Quota(
      quota_request1_, &quota_response,
      [&done_status](Status status) { done_status = status; });

  // done_status should be set to CancelledError immediately since there's no
  // cache.
  EXPECT_EQ(done_status, CancelledError("cancelled error"));

  // Since errors are fail-open, quota_response should contain no error.
  EXPECT_EQ(quota_response.allocate_errors_size(), 0);

  Statistics stat;
  Status stat_status = noncached_client_->GetStatistics(&stat);

  EXPECT_EQ(stat_status, OkStatus());
  EXPECT_EQ(stat.total_called_quotas, 1);
  EXPECT_EQ(stat.send_quotas_by_flush, 0);
  EXPECT_EQ(stat.send_quotas_in_flight, 1);
}

}  // namespace service_control_client
}  // namespace google
