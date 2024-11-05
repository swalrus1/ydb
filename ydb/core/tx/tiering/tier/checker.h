#pragma once
#include "object.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/preparation_controller.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/manager/scheme_manager.h>

namespace NKikimr::NColumnShard::NTiers {

class TTierPreparationActor: public NActors::TActorBootstrapped<TTierPreparationActor> {
private:
    NKikimrSchemeOp::TTieredStorageProperties Properties;
    NMetadata::NModifications::IBuildRequestController::TPtr Controller;
    NMetadata::NModifications::IOperationsManager::TInternalModificationContext Context;

    std::shared_ptr<NMetadata::NSecret::TSnapshot> Secrets;

    void StartChecker();
    TConclusionStatus CheckSecret(const TString& serializedSecret);
protected:
    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev);
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            default:
                AFL_VERIFY(false)("event", ev->ToString());
        }
    }

    void Bootstrap();

    TTierPreparationActor(NKikimrSchemeOp::TTieredStorageProperties properties,
        NMetadata::NModifications::IBuildRequestController::TPtr controller,
        const NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context);
};

}
