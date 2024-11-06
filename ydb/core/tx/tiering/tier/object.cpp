#include "behaviour.h"
#include "object.h"

#include <ydb/core/tx/tiering/tier/checker.h>

#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/uri/uri.h>

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

bool TTierConfig::DeserializeFromProto(const NKikimrSchemeOp::TExternalDataSourceDescription& proto) {
    if (!proto.GetAuth().HasAws()) {
        return false;
    }

    // TODO fix secret owner
    {
        auto makeSecretId = [](const TStringBuf& secret) -> TString {
            if (secret.StartsWith(NMetadata::NSecret::TSecretId::PrefixNoUser) ||   // ... here
                secret.StartsWith(NMetadata::NSecret::TSecretId::PrefixWithUser)) {
                return TString(secret);
            }
            return NMetadata::NSecret::TSecretId("", secret).SerializeToString();   // ... and here
        };
        ProtoConfig.SetSecretKey(makeSecretId(proto.GetAuth().GetAws().GetAwsAccessKeyIdSecretName()));
        ProtoConfig.SetAccessKey(makeSecretId(proto.GetAuth().GetAws().GetAwsSecretAccessKeySecretName()));
    }

    NUri::TUri url;
    if (url.Parse(proto.GetLocation()) != NUri::TState::EParsed::ParsedOK) {
        return false;
    }

    switch (url.GetScheme()) {
        case NUri::TScheme::SchemeEmpty:
            break;
        case NUri::TScheme::SchemeHTTP:
            ProtoConfig.SetScheme(::NKikimrSchemeOp::TS3Settings_EScheme_HTTP);
            break;
        case NUri::TScheme::SchemeHTTPS:
            ProtoConfig.SetScheme(::NKikimrSchemeOp::TS3Settings_EScheme_HTTPS);
            break;
        default:
            return false;
    }

    {
        TStringBuf endpoint;
        TStringBuf bucket;

        TStringBuf host = url.GetHost();
        TStringBuf path = url.GetField(NUri::TField::FieldPath);
        if (path.StartsWith("/") && path.Size() > 1) {
            bucket = path.Skip(1);
            endpoint = host;
        } else {
            if (!path.TrySplit('.', endpoint, bucket)) {
                return false;
            }
        }

        ProtoConfig.SetEndpoint(TString(endpoint));
        ProtoConfig.SetBucket(TString(bucket));
    }

    return true;
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
