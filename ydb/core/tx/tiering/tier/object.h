#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/operations/metadata/properties.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/secret/secret.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NMetadata::NSecret {
class TSnapshot;
}

namespace NKikimr::NColumnShard::NTiers {

class TTierConfig: public NSchemeShard::TMetadataObjectPropertiesBase<TTierConfig> {
private:
    using TTierProto = NKikimrSchemeOp::TS3Settings;
    YDB_READONLY_DEF(TTierProto, ProtoConfig);
    YDB_READONLY_DEF(NKikimrSchemeOp::TCompressionOptions, Compression);

    inline static const TFactory::TRegistrator<TTierConfig> Registrator = NKikimrSchemeOp::EPathTypeTieredStorage;

private:
    static const NKikimrSchemeOp::TTieredStorageProperties& GetProperties(const TProto& proto) {
        AFL_VERIFY(proto.HasTieredStorage())("proto", proto.DebugString());
        return proto.GetTieredStorage();
    }
    static NKikimrSchemeOp::TTieredStorageProperties* MutableProperties(TProto& proto) {
        return proto.MutableTieredStorage();
    }

public:
    TTierConfig() = default;
    TTierConfig(const TTierProto& config, const NKikimrSchemeOp::TCompressionOptions& compression)
        : ProtoConfig(config)
        , Compression(compression) {
    }

    bool DeserializeFromProto(const TProto& proto) override {
        const auto& properties = GetProperties(proto);
        if (!properties.HasObjectStorage()) {
            return false;
        }

        ProtoConfig = properties.GetObjectStorage();
        Compression = properties.GetCompression();
        return true;
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TExternalDataSourceDescription& proto);

    bool ApplyPatch(const TProto& proto) override {
        const auto& properties = GetProperties(proto);
        if (properties.HasCompression()) {
            Compression = properties.GetCompression();
        }
        if (properties.HasObjectStorage()) {
            ProtoConfig = properties.GetObjectStorage();
        }
        return true;
    }

    TProto SerializeToProto() const override {
        TProto serialized;
        auto* properties = MutableProperties(serialized);
        if (Compression.HasCodec() && Compression.GetCodec()) {
            properties->MutableCompression()->CopyFrom(Compression);
        }
        properties->MutableObjectStorage()->CopyFrom(ProtoConfig);
        return serialized;
    }

    NMetadata::NSecret::TSecretIdOrValue GetAccessKey() const {
        auto accessKey =
            NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetSecretableAccessKey(), ProtoConfig.GetAccessKey());
        if (!accessKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *accessKey;
    }

    NMetadata::NSecret::TSecretIdOrValue GetSecretKey() const {
        auto secretKey =
            NMetadata::NSecret::TSecretIdOrValue::DeserializeFromOptional(ProtoConfig.GetSecretableSecretKey(), ProtoConfig.GetSecretKey());
        if (!secretKey) {
            return NMetadata::NSecret::TSecretIdOrValue::BuildEmpty();
        }
        return *secretKey;
    }

    NJson::TJsonValue SerializeConfigToJson() const;

    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    NKikimrSchemeOp::TS3Settings GetPatchedConfig(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) const;

    bool IsSame(const TTierConfig& item) const;
    NJson::TJsonValue GetDebugJson() const;
    static TString GetTypeId() {
        return "TIERED_STORAGE";
    }
    static TString GetLocalStorageDirectory() {
        return ".tiered_storages";
    }
};
}
