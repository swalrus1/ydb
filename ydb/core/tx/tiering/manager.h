#pragma once
#include "common.h"

#include "abstract/manager.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>
#include <ydb/services/metadata/secret/snapshot.h>
#include <ydb/services/metadata/service.h>

#include <functional>

namespace NKikimr::NColumnShard {
namespace NTiers {

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TOlapColumn::TSerializer& serializerProto);
NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compressionProto);

class TManager {
private:
    ui64 TabletId = 0;
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY_DEF(TTierConfig, Config);
    YDB_READONLY_DEF(NActors::TActorId, StorageActorId);
    std::optional<NKikimrSchemeOp::TS3Settings> S3Settings;
public:
    const NKikimrSchemeOp::TS3Settings& GetS3Settings() const {
        Y_ABORT_UNLESS(S3Settings);
        return *S3Settings;
    }

    TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TString& tierName, const TTierConfig& config);

    TManager& Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    bool Stop();
    bool Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
};
}

class TTiersManager: public ITiersManager {
private:
    class TTierRef: public TMoveOnly {
    private:
        YDB_READONLY_DEF(TString, TierName);
        THashMap<TString, ui64>* RefCounters;

    public:
        TTierRef(const TString& tierName, THashMap<TString, ui64>& refCounters)
            : TierName(tierName)
            , RefCounters(&refCounters) {
            ++(*RefCounters)[tierName];
        }

        TTierRef(TTierRef&& other)
            : TierName(other.TierName)
            , RefCounters(other.RefCounters) {
            other.RefCounters = nullptr;
        }
        TTierRef& operator=(TTierRef&& other) {
            std::swap(RefCounters, other.RefCounters);
            std::swap(TierName, other.TierName);
            return *this;
        }

        ~TTierRef() {
            if (RefCounters) {
                auto findTier = RefCounters->FindPtr(TierName);
                AFL_VERIFY(findTier);
                AFL_VERIFY(*findTier);
                --*findTier;
            }
        }
    };

private:
    class TActor;
    friend class TActor;
    using TManagers = std::map<TString, NTiers::TManager>;

    ui64 TabletId = 0;
    const TActorId TabletActorId;
    std::function<void(const TActorContext& ctx)> ShardCallback;
    IActor* Actor = nullptr;
    TManagers Managers;

    using TTierRefCount = THashMap<TString, ui64>;
    using TTierRefsByPathId = THashMap<ui64, std::vector<TTierRef>>;
    YDB_READONLY_DEF(TTierRefCount, TierRefCount);
    YDB_READONLY_DEF(TTierRefsByPathId, UsedTiers);

    using TTierByName = THashMap<TString, NTiers::TTierConfig>;
    YDB_ACCESSOR_DEF(TTierByName, Tiers);
    YDB_ACCESSOR_DEF(std::shared_ptr<NMetadata::NSecret::TSnapshot>, Secrets);

private:
    void OnConfigsUpdated(bool notifyShard = true);

public:
    TTiersManager(const ui64 tabletId, const TActorId& tabletActorId, std::function<void(const TActorContext& ctx)> shardCallback = {})
        : TabletId(tabletId)
        , TabletActorId(tabletActorId)
        , ShardCallback(shardCallback)
        , Secrets(std::make_shared<NMetadata::NSecret::TSnapshot>(TInstant::Zero())) {
    }
    TActorId GetActorId() const;
    void EnablePathId(const ui64 pathId, const THashSet<TString>& usedTiers);
    void DisablePathId(const ui64 pathId);

    void UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets);
    void UpdateTierConfig(const NTiers::TTierConfig& config, const TString& tierName, bool notifyShard = true);

    TString DebugString();

    TTiersManager& Start(std::shared_ptr<TTiersManager> ownerPtr);
    TTiersManager& Stop(const bool needStopActor);
    virtual const std::map<TString, NTiers::TManager>& GetManagers() const override {
        return Managers;
    }
    virtual const NTiers::TManager* GetManagerOptional(const TString& tierId) const override;
};

}
