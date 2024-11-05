#include "update.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr::NColumnShard::NTiers {

TTieredStorageValidator::TSchemeConclusionStatus TTieredStorageValidator::ValidatePath() const {
    const TString storagePath = JoinPath({ AppDataVerified().TenantName, NColumnShard::NTiers::TTierConfig::GetLocalStorageDirectory() });
    if (!IsEqualPaths(Parent.PathString(), storagePath)) {
        return TSchemeConclusionStatus::Fail("Tiered storages must be placed at " + storagePath + ", got " + Parent.PathString());
    }
    return TSchemeConclusionStatus::Success();
}

TTieredStorageValidator::TSchemeConclusionStatus TTieredStorageValidator::ValidateProperties(
    const NSchemeShard::IMetadataObjectProperties::TPtr object) const {
    const auto tier = std::dynamic_pointer_cast<TTierConfig>(object);
    AFL_VERIFY(tier);
    if (tier->GetProtoConfig().GetEndpoint().Empty()) {
        return TSchemeConclusionStatus::Fail("Empty endpoint");
    }
    if (tier->GetProtoConfig().GetBucket().Empty()) {
        return TSchemeConclusionStatus::Fail("Empty bucket");
    }
    if (tier->GetCompression().HasCodec()) {
        return TSchemeConclusionStatus::Fail("Compression in tiered storage is not supported");
    }
    return TSchemeConclusionStatus::Success();
}

TTieredStorageValidator::TSchemeConclusionStatus TTieredStorageValidator::ValidateAlter(
    const NSchemeShard::IMetadataObjectProperties::TPtr, const NKikimrSchemeOp::TMetadataObjectProperties&) const {
    return TSchemeConclusionStatus::Success();
}

TTieredStorageValidator::TSchemeConclusionStatus TTieredStorageValidator::ValidateDrop(
    const NSchemeShard::IMetadataObjectProperties::TPtr) const {
    if (auto tables = Ctx.SS->ColumnTables.GetTablesWithTier(Name); !tables.empty()) {
        const TString tableString = NSchemeShard::TPath::Init(*tables.begin(), Ctx.SS).PathString();
        return TSchemeConclusionStatus::Fail("Tier is in use by column table: " + tableString);
    }

    return TSchemeConclusionStatus::Success();
}
}   // namespace NKikimr::NColumnShard::NTiers
