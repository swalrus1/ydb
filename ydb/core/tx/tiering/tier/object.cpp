#include "object.h"
#include "behaviour.h"

#include <ydb/core/tx/tiering/tier/checker.h>

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard::NTiers {

NMetadata::IClassBehaviour::TPtr TTierConfig::GetBehaviour() {
    static std::shared_ptr<TTierConfigBehaviour> result = std::make_shared<TTierConfigBehaviour>();
    return result;
}

NKikimrSchemeOp::TS3Settings TTierConfig::GetPatchedConfig(
    std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const {
    auto config = ProtoConfig;
    if (secrets) {
        if (!secrets->GetSecretValue(GetAccessKey(), *config.MutableAccessKey())) {
            ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot read access key secret for " << GetAccessKey().DebugString();
        }
        if (!secrets->GetSecretValue(GetSecretKey(), *config.MutableSecretKey())) {
            ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot read secret key secret for " << GetSecretKey().DebugString();
        }
    }
    return config;
}

NJson::TJsonValue TTierConfig::SerializeConfigToJson() const {
    NJson::TJsonValue result;
    NProtobufJson::Proto2Json(ProtoConfig, result);
    return result;
}

bool TTierConfig::IsSame(const TTierConfig& item) const {
    return ProtoConfig.SerializeAsString() == item.ProtoConfig.SerializeAsString();
}

}
