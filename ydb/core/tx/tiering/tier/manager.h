#pragma once
#include "object.h"

#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersManager: public NMetadata::NModifications::TSchemeObjectOperationsManager {
private:
    inline static const TString KeySecretKey = "secretKey";
    inline static const TString KeyAccessKey = "accessKey";
    inline static const TString KeyEndpoint = "endpoint";
    inline static const TString KeyBucket = "bucket";
    inline static const TString KeyCompressionCodec = "compression";
    inline static const TString KeyCompressionLevel = "compressionLevel";

    inline static const THashMap<TString, NKikimrSchemeOp::EColumnCodec> CodecByName = {
        { "LZ4", NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4 }, { "ZSTD", NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD }
    };

    static std::optional<NKikimrSchemeOp::EColumnCodec> ParseCodec(const TString& in) {
        if (auto findCodec = CodecByName.FindPtr(in)) {
            return *findCodec;
        }
        return std::nullopt;
    }

protected:
    void DoBuildRequestFromSettings(const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context,
        IBuildRequestController::TPtr controller) const override;

    TString GetLocalStorageDirectory() const override {
        return TTierConfig::GetLocalStorageDirectory();
    }
};
}
