#include "checker.h"

#include <ydb/services/metadata/secret/fetcher.h>

namespace NKikimr::NColumnShard::NTiers {

TConclusionStatus TTierPreparationActor::CheckSecret(const TString& serializedSecret) {
    const auto secret = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(serializedSecret);
    if (!secret) {
        return TConclusionStatus::Fail("Invalid secret format: " + serializedSecret);
    }
    if (!Secrets->CheckSecretAccess(*secret, Context.GetExternalData().GetUserToken())) {
        return TConclusionStatus::Fail("No access for secret: " + serializedSecret);
    }
    return TConclusionStatus::Success();
}

void TTierPreparationActor::StartChecker() {
    AFL_VERIFY(Secrets);
    auto g = PassAwayGuard();
    if (Properties.GetObjectStorage().HasAccessKey()) {
        if (const auto status = CheckSecret(Properties.GetObjectStorage().GetAccessKey()); status.IsFail()) {
            Controller->OnBuildProblem("Invalid access key: " + status.GetErrorMessage());
            return;
        }
    }
    if (Properties.GetObjectStorage().HasSecretKey()) {
        if (const auto status = CheckSecret(Properties.GetObjectStorage().GetSecretKey()); status.IsFail()) {
            Controller->OnBuildProblem("Invalid secret key: " + status.GetErrorMessage());
            return;
        }
    }
    NKikimrSchemeOp::TMetadataObjectProperties metadataProperties;
    *metadataProperties.MutableTieredStorage() = std::move(Properties);
    Controller->OnBuildFinished(std::move(metadataProperties));
}

void TTierPreparationActor::Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    if (auto snapshot = ev->Get()->GetSnapshotPtrAs<NMetadata::NSecret::TSnapshot>()) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "snapshot_fetched")("type", "secret");
        Secrets = snapshot;
    } else {
        Y_ABORT_UNLESS(false);
    }
    StartChecker();
}

void TTierPreparationActor::Bootstrap() {
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>()));
    Become(&TThis::StateMain);
}

TTierPreparationActor::TTierPreparationActor(NKikimrSchemeOp::TTieredStorageProperties properties,
    NMetadata::NModifications::IBuildRequestController::TPtr controller,
    const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context)
    : Properties(std::move(properties))
    , Controller(controller)
    , Context(context) {
}
}
