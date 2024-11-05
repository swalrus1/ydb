#pragma once

#include <ydb/core/tx/schemeshard/operations/metadata/behaviour.h>

namespace NKikimr::NColumnShard::NTiers {

class TTieredStorageValidator final: public NSchemeShard::NOperations::IMetadataUpdateValidator {
public:
    TSchemeConclusionStatus ValidatePath() const override;
    TSchemeConclusionStatus ValidateProperties(const NSchemeShard::IMetadataObjectProperties::TPtr object) const override;
    TSchemeConclusionStatus ValidateAlter(
        const NSchemeShard::IMetadataObjectProperties::TPtr object, const NKikimrSchemeOp::TMetadataObjectProperties& request) const override;
    TSchemeConclusionStatus ValidateDrop(const NSchemeShard::IMetadataObjectProperties::TPtr object) const override;

    using IMetadataUpdateValidator::IMetadataUpdateValidator;
};

class TTieredStorageUpdateBehaviour final: public NSchemeShard::NOperations::IMetadataUpdateBehaviour {
private:
    inline static constexpr NKikimrSchemeOp::EPathType PathType = NKikimrSchemeOp::EPathTypeTieredStorage;
    inline static constexpr NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase PropertiesImplCase =
        NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase::kTieredStorage;

    inline static const TFactoryByPropertiesImpl::TRegistrator<TTieredStorageUpdateBehaviour> RegistratorByPropertiesImpl = PropertiesImplCase;
    inline static const TFactoryByPath::TRegistrator<TTieredStorageUpdateBehaviour> RegistratorByPath = PathType;

public:
    NKikimrSchemeOp::TMetadataObjectProperties::PropertiesImplCase GetPropertiesImplCase() const {
        return PropertiesImplCase;
    }
    NKikimrSchemeOp::EPathType GetObjectPathType() const {
        return PathType;
    }
    NSchemeShard::ESimpleCounters GetCounterType() const {
        return NSchemeShard::COUNTER_TIERED_STORAGE_COUNT;
    }

    TTieredStorageValidator::IMetadataUpdateValidator::TPtr MakeValidator(
        const NSchemeShard::TPath& parent, const TString& objectName, const NSchemeShard::TOperationContext& ctx) {
        return std::make_shared<TTieredStorageValidator>(parent, objectName, ctx);
    }

    using IMetadataUpdateBehaviour::IMetadataUpdateBehaviour;
};

}   // namespace NKikimr::NColumnShard::NTiers
