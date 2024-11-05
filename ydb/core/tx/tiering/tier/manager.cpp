#include "manager.h"

#include <ydb/core/tx/tiering/tier/behaviour.h>
#include <ydb/core/tx/tiering/tier/checker.h>
#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr::NColumnShard::NTiers {

void TTiersManager::DoBuildRequestFromSettings(
    const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IBuildRequestController::TPtr controller) const {
    if (HasAppData() && !AppDataVerified().FeatureFlags.GetEnableTieringInColumnShard()) {
        controller->OnBuildProblem("Tiering functionality is disabled for OLAP tables.");
        return;
    }

    TString defaultUserId;
    if (context.GetExternalData().GetUserToken()) {
        defaultUserId = context.GetExternalData().GetUserToken()->GetUserSID();
    }

    NKikimrSchemeOp::TTieredStorageProperties properties;

    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyAccessKey)) {
        auto key = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(*fValue, defaultUserId);
        if (!key) {
            controller->OnBuildProblem("Access key is incorrect: " + *fValue + " for userId: " + defaultUserId);
            return;
        }
        *properties.MutableObjectStorage()->MutableAccessKey() = key->SerializeToString();
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeySecretKey)) {
        auto key = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(*fValue, defaultUserId);
        if (!key) {
            controller->OnBuildProblem("Secret key is incorrect: " + *fValue + " for userId: " + defaultUserId);
            return;
        }
        *properties.MutableObjectStorage()->MutableSecretKey() = key->SerializeToString();
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyEndpoint)) {
        if (fValue->Empty()) {
            controller->OnBuildProblem("Empty endpoint");
            return;
        }
        *properties.MutableObjectStorage()->MutableEndpoint() = *fValue;
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyBucket)) {
        if (fValue->Empty()) {
            controller->OnBuildProblem("Empty bucket");
            return;
        }
        *properties.MutableObjectStorage()->MutableBucket() = *fValue;
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyCompressionCodec)) {
        if (auto codec = ParseCodec(*fValue)) {
            properties.MutableCompression()->SetCodec(*codec);
        } else {
            controller->OnBuildProblem("Unsupported compression codec: " + *fValue);
            return;
        }
    }
    if (auto fValue = settings.GetFeaturesExtractor().Extract(KeyCompressionLevel)) {
        ui32 level;
        if (!TryFromString(*fValue, level)) {
            controller->OnBuildProblem("Cannot parse compression level: " + *fValue);
            return;
        }
        properties.MutableCompression()->SetLevel(level);
    }

    if (!settings.GetFeaturesExtractor().IsFinished()) {
        controller->OnBuildProblem("undefined params: " + settings.GetFeaturesExtractor().GetRemainedParamsString());
        return;
    }

    auto* actorSystem = context.GetExternalData().GetActorSystem();
    AFL_VERIFY(actorSystem);
    actorSystem->Register(new TTierPreparationActor(std::move(properties), controller, context));
}
}

