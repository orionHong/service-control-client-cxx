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

#include "src/service_control_client_impl.h"
#include "src/quota_aggregator_impl.h"

#include "google/protobuf/stubs/logging.h"
#include "utils/thread.h"

#include <climits>

using ::google::api::servicecontrol::v1::CheckRequest;
using ::google::api::servicecontrol::v1::CheckResponse;
using ::google::api::servicecontrol::v1::AllocateQuotaRequest;
using ::google::api::servicecontrol::v1::AllocateQuotaResponse;
using ::google::api::servicecontrol::v1::ReportRequest;
using ::google::api::servicecontrol::v1::ReportResponse;
using ::google::protobuf::util::OkStatus;
using ::google::protobuf::util::Status;
using ::google::protobuf::util::StatusCode;

namespace google {
namespace service_control_client {

// Check if a status represents a fail-open error.
bool IsQuotaFailOpenError(const absl::Status& status) {
  switch (status.code()) {
    case StatusCode::kUnknown:
    case StatusCode::kUnimplemented:
    case StatusCode::kUnavailable:
    case StatusCode::kDeadlineExceeded:
    case StatusCode::kInternal:
      return true;
    default:
      return false;
  }
}


ServiceControlClientImpl::ServiceControlClientImpl(
    const std::string& service_name, const std::string& service_config_id,
    ServiceControlClientOptions& options)
    : service_name_(service_name) {
  check_aggregator_ =
      CreateCheckAggregator(service_name, service_config_id,
                            options.check_options, options.metric_kinds);

  quota_aggregator_ = CreateAllocateQuotaAggregator(
      service_name, service_config_id, options.quota_options);

  report_aggregator_ =
      CreateReportAggregator(service_name, service_config_id,
                             options.report_options, options.metric_kinds);

  quota_transport_ = options.quota_transport;
  check_transport_ = options.check_transport;
  report_transport_ = options.report_transport;

  total_called_checks_ = 0;
  send_checks_by_flush_ = 0;
  send_checks_in_flight_ = 0;

  total_called_quotas_ = 0;
  send_quotas_by_flush_ = 0;
  send_quotas_in_flight_ = 0;

  total_called_reports_ = 0;
  send_reports_by_flush_ = 0;
  send_reports_in_flight_ = 0;
  send_report_operations_ = 0;

  check_aggregator_->SetFlushCallback(
      std::bind(&ServiceControlClientImpl::CheckFlushCallback, this,
                std::placeholders::_1));

  quota_aggregator_->SetFlushCallback(
      std::bind(&ServiceControlClientImpl::AllocateQuotaFlushCallback, this,
                std::placeholders::_1));

  report_aggregator_->SetFlushCallback(
      std::bind(&ServiceControlClientImpl::ReportFlushCallback, this,
                std::placeholders::_1));

  int flush_interval = GetNextFlushInterval();
  if (options.periodic_timer && flush_interval > 0) {
    // Class members cannot be captured in lambda. We need to make a copy to
    // support C++11.
    std::shared_ptr<CheckAggregator> check_aggregator_copy = check_aggregator_;
    std::shared_ptr<QuotaAggregator> quota_aggregator_copy = quota_aggregator_;
    std::shared_ptr<ReportAggregator> report_aggregator_copy =
        report_aggregator_;

    flush_timer_ = options.periodic_timer(
        flush_interval, [check_aggregator_copy, quota_aggregator_copy,
                         report_aggregator_copy]() {

          Status status = check_aggregator_copy->Flush();
          if (!status.ok()) {
            GOOGLE_LOG(ERROR) << "Failed in Check::Flush() "
                              << status.message();
          }

          status = quota_aggregator_copy->Flush();
          if (!status.ok()) {
            GOOGLE_LOG(ERROR) << "Failed in AllocateQuota::Flush() "
                              << status.message();
          }

          status = report_aggregator_copy->Flush();
          if (!status.ok()) {
            GOOGLE_LOG(ERROR) << "Failed in Report::Flush() "
                              << status.message();
          }
        });
  }
}

ServiceControlClientImpl::~ServiceControlClientImpl() {
  // Flush out all cached data
  (void)FlushAll();
  if (flush_timer_) {
    flush_timer_->Stop();
  }

  // Disconnects all callback functions since this object is going away.
  // There could be some on_check_done() flying around. Each of them is
  // holding a ref_count to check_aggregator so check_aggregator is still
  // valid until all on_check_done() are called.
  // Each on_check_done() may call check_aggregator->CacheResponse() which
  // may call the flush callback. But since flush callback is disconnected,
  // we are OK.
  check_aggregator_->SetFlushCallback(NULL);
  quota_aggregator_->SetFlushCallback(NULL);
  report_aggregator_->SetFlushCallback(NULL);
}

void ServiceControlClientImpl::AllocateQuotaFlushCallback(
    const AllocateQuotaRequest& quota_request) {
  AllocateQuotaRequest* quota_request_copy =
      new AllocateQuotaRequest(quota_request);
  AllocateQuotaResponse* quota_response = new AllocateQuotaResponse;

  quota_transport_(*quota_request_copy, quota_response,
                   [this, quota_request_copy, quota_response](Status status) {
                     if (status.ok()) {
                       (void)this->quota_aggregator_->CacheResponse(
                         *quota_request_copy, *quota_response);
                     } else if (IsQuotaFailOpenError(status)) {
                       // Cache dummy response for fail open.
                       GOOGLE_LOG(ERROR) << "Failed in AllocateQuota call: "
                                         << status.message()
                                         << ", but treated as fail-open";
                       AllocateQuotaResponse dummy_response;
                       (void)this->quota_aggregator_->CacheResponse(
                         *quota_request_copy, dummy_response);
                     } else {
                       // Cache error response for fail close.
                       AllocateQuotaResponse error_response;
                       QuotaError client_side_error;
                       *client_side_error.mutable_status() = util::SaveStatusAsRpcStatus(status);
                       *error_response.add_allocate_errors() = client_side_error;
                       (void)this->quota_aggregator_->CacheResponse(
                         *quota_request_copy, error_response);
                     }

                     delete quota_request_copy;
                     delete quota_response;
                   });

  ++send_quotas_by_flush_;
}

void ServiceControlClientImpl::CheckFlushCallback(
    const CheckRequest& check_request) {
  CheckResponse* check_response = new CheckResponse;
  check_transport_(check_request, check_response,
                   [check_response](Status status) {
                     delete check_response;
                     if (!status.ok()) {
                       GOOGLE_LOG(ERROR) << "Failed in Check call: "
                                         << status.message();
                     }
                   });
  ++send_checks_by_flush_;
}

void ServiceControlClientImpl::ReportFlushCallback(
    const ReportRequest& report_request) {
  ReportResponse* report_response = new ReportResponse;
  report_transport_(report_request, report_response,
                    [report_response](Status status) {
                      delete report_response;
                      if (!status.ok()) {
                        GOOGLE_LOG(ERROR) << "Failed in Report call: "
                                          << status.message();
                      }
                    });
  ++send_reports_by_flush_;
  send_report_operations_ += report_request.operations_size();
}

void ServiceControlClientImpl::Check(const CheckRequest& check_request,
                                     CheckResponse* check_response,
                                     DoneCallback on_check_done,
                                     TransportCheckFunc check_transport) {
  ++total_called_checks_;
  if (check_transport == NULL) {
    on_check_done(Status(StatusCode::kInvalidArgument, "transport is NULL."));
    return;
  }

  Status status = check_aggregator_->Check(check_request, check_response);
  if (status.code() == StatusCode::kNotFound) {
    // Makes a copy of check_request so that on_done() callback can use
    // it to call CacheResponse.
    CheckRequest* check_request_copy = new CheckRequest(check_request);
    std::shared_ptr<CheckAggregator> check_aggregator_copy = check_aggregator_;
    check_transport(*check_request_copy, check_response,
                    [check_aggregator_copy, check_request_copy, check_response,
                     on_check_done](Status status) {
                      if (status.ok()) {
                        (void)check_aggregator_copy->CacheResponse(
                            *check_request_copy, *check_response);
                      } else {
                        GOOGLE_LOG(ERROR) << "Failed in Check call: "
                                          << status.message();
                      }
                      delete check_request_copy;
                      on_check_done(status);
                    });
    ++send_checks_in_flight_;
    return;
  }
  on_check_done(status);
}

void ServiceControlClientImpl::Check(const CheckRequest& check_request,
                                     CheckResponse* check_response,
                                     DoneCallback on_check_done) {
  Check(check_request, check_response, on_check_done, check_transport_);
}

void ServiceControlClientImpl::Quota(const AllocateQuotaRequest& quota_request,
                                     AllocateQuotaResponse* quota_response,
                                     DoneCallback on_quota_done,
                                     TransportQuotaFunc quota_transport) {
  ++total_called_quotas_;
  if (quota_transport == NULL) {
    on_quota_done(Status(StatusCode::kInvalidArgument, "transport is NULL."));
    return;
  }

  Status status = quota_aggregator_->Quota(quota_request, quota_response);
  if (status.code() == StatusCode::kNotFound) {
    // Makes a copy of check_request so that on_done() callback can use
    // it to call CacheResponse.
    AllocateQuotaRequest* quota_request_copy =
        new AllocateQuotaRequest(quota_request);

    std::shared_ptr<QuotaAggregator> quota_aggregator_copy = quota_aggregator_;
    quota_transport(*quota_request_copy, quota_response,
                    [quota_aggregator_copy, quota_request_copy,
                     quota_response, on_quota_done](Status status) {

                      if (status.ok()) {
                        (void)quota_aggregator_copy->CacheResponse(
                            *quota_request_copy, *quota_response);
                      } else {
                        // on network error, failed open, reset in_flight flag
                        // to false
                        AllocateQuotaResponse dummy_response;
                        (void)quota_aggregator_copy->CacheResponse(
                            *quota_request_copy, dummy_response);

                        GOOGLE_LOG(ERROR) << "Failed in Quota call: "
                                          << status.message();
                      }

                      delete quota_request_copy;

                      on_quota_done(status);
                    });

    ++send_quotas_in_flight_;
    return;
  } else {
    // OkStatus(), return response status from AllocateQuotaResponse
    on_quota_done(status);
  }
}

// An async quota call.
void ServiceControlClientImpl::Quota(const AllocateQuotaRequest& quota_request,
                                     AllocateQuotaResponse* quota_response,
                                     DoneCallback on_quota_done) {
  Quota(quota_request, quota_response, on_quota_done, quota_transport_);
}

void ServiceControlClientImpl::Report(const ReportRequest& report_request,
                                      ReportResponse* report_response,
                                      DoneCallback on_report_done,
                                      TransportReportFunc report_transport) {
  ++total_called_reports_;
  if (report_transport == NULL) {
    on_report_done(Status(StatusCode::kInvalidArgument, "transport is NULL."));
    return;
  }

  Status status = report_aggregator_->Report(report_request);
  if (status.code() == StatusCode::kNotFound) {
    report_transport(report_request, report_response, on_report_done);
    ++send_reports_in_flight_;
    send_report_operations_ += report_request.operations_size();
    return;
  }
  on_report_done(status);
}

void ServiceControlClientImpl::Report(const ReportRequest& report_request,
                                      ReportResponse* report_response,
                                      DoneCallback on_report_done) {
  Report(report_request, report_response, on_report_done, report_transport_);
}

Status ServiceControlClientImpl::GetStatistics(Statistics* stat) const {
  stat->total_called_checks = total_called_checks_;
  stat->send_checks_by_flush = send_checks_by_flush_;
  stat->send_checks_in_flight = send_checks_in_flight_;

  stat->total_called_quotas = total_called_quotas_;
  stat->send_quotas_by_flush = send_quotas_by_flush_;
  stat->send_quotas_in_flight = send_quotas_in_flight_;

  stat->total_called_reports = total_called_reports_;
  stat->send_reports_by_flush = send_reports_by_flush_;
  stat->send_reports_in_flight = send_reports_in_flight_;
  stat->send_report_operations = send_report_operations_;
  return OkStatus();
}

int ServiceControlClientImpl::GetNextFlushInterval() {
  int check_interval = check_aggregator_->GetNextFlushInterval();
  int quota_interval = quota_aggregator_->GetNextFlushInterval();
  int report_interval = report_aggregator_->GetNextFlushInterval();

  check_interval =
      (check_interval < 0) ? std::numeric_limits<int>::max() : check_interval;
  quota_interval =
      (quota_interval < 0) ? std::numeric_limits<int>::max() : quota_interval;
  report_interval =
      (report_interval < 0) ? std::numeric_limits<int>::max() : report_interval;

  return std::min(check_interval, std::min(quota_interval, report_interval));
}

Status ServiceControlClientImpl::Flush() {
  Status check_status = check_aggregator_->Flush();
  Status quota_status = quota_aggregator_->Flush();
  Status report_status = report_aggregator_->Flush();

  if (!check_status.ok()) {
    return check_status;
  } else if (!quota_status.ok()) {
    return quota_status;
  } else {
    return report_status;
  }
}

Status ServiceControlClientImpl::FlushAll() {
  Status check_status = check_aggregator_->FlushAll();
  Status quota_status = quota_aggregator_->FlushAll();
  Status report_status = report_aggregator_->FlushAll();

  if (!check_status.ok()) {
    return check_status;
  } else if (!quota_status.ok()) {
    return quota_status;
  } else {
    return report_status;
  }
}

// Creates a ServiceControlClient object.
std::unique_ptr<ServiceControlClient> CreateServiceControlClient(
    const std::string& service_name, const std::string& service_config_id,
    ServiceControlClientOptions& options) {
  return std::unique_ptr<ServiceControlClient>(
      new ServiceControlClientImpl(service_name, service_config_id, options));
}

}  // namespace service_control_client
}  // namespace google
