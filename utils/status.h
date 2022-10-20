/* Copyright 2022 Google Inc. All Rights Reserved.

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

#ifndef GOOGLE_SERVICE_CONTROL_CLIENT_UTILS_STATUS_H_
#define GOOGLE_SERVICE_CONTROL_CLIENT_UTILS_STATUS_H_

#include "google/rpc/status.pb.h"

namespace google {
namespace service_control_client {

// This helper function converts google::protobuf::util::Status to google::rpc::Status.
google::rpc::Status SaveStatusAsRpcStatus(const google::protobuf::util::Status& status) {
    google::rpc::Status ret;
    ret.set_code(static_cast<int>(status.code()));
    ret.set_message(status.message().data(), status.message().size());
    return ret;
}

}  // namespace service_control_client
}  // namespace google

#endif
