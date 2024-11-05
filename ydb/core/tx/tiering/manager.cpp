#include "common.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/tiering/fetcher/watch.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/services/metadata/secret/fetcher.h>

#include <util/string/vector.h>

namespace NKikimr::NColumnShard {

class TTiersManager::TActor: public TActorBootstrapped<TTiersManager::TActor> {
private:
    std::shared_ptr<TTiersManager> Owner;
    NMetadata::NFetcher::ISnapshotsFetcher::TPtr SecretsFetcher;
    TActorId TieredStorageFetcher;

private:
    TActorId GetExternalDataActorId() const {
        return NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
    }

    void ScheduleRetryWatchObjects(std::unique_ptr<NTiers::TEvWatchTieredStorages> ev) const {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiers_manager")("event", "retry_watch_objects");
        constexpr static const TDuration RetryInterval = TDuration::Seconds(1);
        ActorContext().Schedule(RetryInterval, std::make_unique<IEventHandle>(SelfId(), TieredStorageFetcher, ev.release()));
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            hFunc(NTiers::TEvNotifyTieredStorageUpdated, Handle);
            hFunc(NTiers::TEvNotifyTieredStorageDeleted, Handle);
            hFunc(NTiers::TEvTieredStorageResolutionFailed, Handle);
            hFunc(NTiers::TEvWatchTieredStorages, Handle);
            default:
                break;
        }
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        auto snapshot = ev->Get()->GetSnapshot();
        if (auto secrets = std::dynamic_pointer_cast<NMetadata::NSecret::TSnapshot>(snapshot)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "TEvRefreshSubscriberData")("snapshot", "secrets");
            Owner->UpdateSecretsSnapshot(secrets);
        } else {
            Y_ABORT_UNLESS(false, "unexpected snapshot");
        }
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& /*ev*/) {
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvUnsubscribeExternal(SecretsFetcher));
        PassAway();
    }

    void Handle(NTiers::TEvNotifyTieredStorageUpdated::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_updated")("name", ev->Get()->GetName())("type", "TIERED_STORAGE");
        Owner->UpdateTierConfig(ev->Get()->GetConfig(), ev->Get()->GetName());
    }

    void Handle(NTiers::TEvNotifyTieredStorageDeleted::TPtr& ev) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_manager")("event", "object_deleted")("name", ev->Get()->GetName())("type", "TIERED_STORAGE");
    }

    void Handle(NTiers::TEvTieredStorageResolutionFailed::TPtr& ev) {
        const TString name = ev->Get()->GetName();
        switch (ev->Get()->GetReason()) {
            case NTiers::TBaseEvObjectResolutionFailed::NOT_FOUND:
                AFL_WARN(NKikimrServices::TX_TIERING)("event", "object_not_found")("name", name)("type", "TIERING_RULE");
                break;
            case NTiers::TBaseEvObjectResolutionFailed::LOOKUP_ERROR:
                ScheduleRetryWatchObjects(std::make_unique<NTiers::TEvWatchTieredStorages>(std::vector<TString>({ name })));
                break;
            case NTiers::TBaseEvObjectResolutionFailed::UNEXPECTED_KIND:
                AFL_VERIFY(false)("name", name);
        }
    }

    void Handle(NTiers::TEvWatchTieredStorages::TPtr& ev) {
        Send(TieredStorageFetcher, ev->Release());
    }

public:
    TActor(std::shared_ptr<TTiersManager> owner)
        : Owner(owner)
        , SecretsFetcher(std::make_shared<NMetadata::NSecret::TSnapshotsFetcher>())
    {
    }

    void Bootstrap() {
        AFL_INFO(NKikimrServices::TX_TIERING)("event", "start_subscribing_metadata");
        TieredStorageFetcher = Register(new TTieringWatcher(SelfId()));
        Send(GetExternalDataActorId(), new NMetadata::NProvider::TEvSubscribeExternal(SecretsFetcher));
        Become(&TThis::StateMain);
    }

    ~TActor() {
        Owner->Stop(false);
    }
};

namespace NTiers {

TManager& TManager::Restart(const TTierConfig& config, std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Restarting tier '" << GetTierName() << "' at tablet " << TabletId;
    if (Config.IsSame(config)) {
        return *this;
    }
    Stop();
    Config = config;
    Start(secrets);
    return *this;
}

bool TManager::Stop() {
    S3Settings.reset();
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << GetTierName() << "' stopped at tablet " << TabletId;
    return true;
}

bool TManager::Start(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_VERIFY(!S3Settings)("tier", GetTierName())("event", "already started");
    S3Settings = Config.GetPatchedConfig(secrets);
    ALS_DEBUG(NKikimrServices::TX_TIERING) << "Tier '" << GetTierName() << "' started at tablet " << TabletId;
    return true;
}

TManager::TManager(const ui64 tabletId, const NActors::TActorId& tabletActorId, const TString& tierName, const TTierConfig& config)
    : TabletId(tabletId)
    , TabletActorId(tabletActorId)
    , TierName(tierName)
    , Config(config)
{
}

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TCompressionOptions& compressionProto) {
    NArrow::NSerialization::TSerializerContainer container;
    container.DeserializeFromProto(compressionProto).Validate();
    return container;
}

NArrow::NSerialization::TSerializerContainer ConvertCompression(const NKikimrSchemeOp::TOlapColumn::TSerializer& serializerProto) {
    NArrow::NSerialization::TSerializerContainer container;
    AFL_VERIFY(container.DeserializeFromProto(serializerProto));
    return container;
}
}

void TTiersManager::OnConfigsUpdated(bool notifyShard) {
    for (auto itSelf = Managers.begin(); itSelf != Managers.end(); ) {
        auto it = Tiers.find(itSelf->first);
        if (it == Tiers.end()) {
            itSelf->second.Stop();
            itSelf = Managers.erase(itSelf);
        } else {
            itSelf->second.Restart(it->second, Secrets);
            ++itSelf;
        }
    }

    for (const auto& [tierName, refCount] : TierRefCount) {
        AFL_VERIFY(refCount);
        auto findConfig = Tiers.FindPtr(tierName);
        if (findConfig && !Managers.contains(tierName)) {
            AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "starting_tier_manager")("tier", tierName);
            NTiers::TManager localManager(TabletId, TabletActorId, tierName, *findConfig);
            auto itManager = Managers.emplace(tierName, std::move(localManager)).first;
            itManager->second.Start(Secrets);
        }
    }

    if (notifyShard && ShardCallback && TlsActivationContext) {
        ShardCallback(TActivationContext::AsActorContext());
    }

    AFL_DEBUG(NKikimrServices::TX_TIERING)("event", "configs_updated")("configs", DebugString());
}

TTiersManager& TTiersManager::Start(std::shared_ptr<TTiersManager> ownerPtr) {
    Y_ABORT_UNLESS(!Actor);
    Actor = new TTiersManager::TActor(ownerPtr);
    TActivationContext::AsActorContext().RegisterWithSameMailbox(Actor);
    return *this;
}

TTiersManager& TTiersManager::Stop(const bool needStopActor) {
    if (!Actor) {
        return *this;
    }
    if (TlsActivationContext && needStopActor) {
        TActivationContext::AsActorContext().Send(Actor->SelfId(), new NActors::TEvents::TEvPoison);
    }
    Actor = nullptr;
    for (auto&& i : Managers) {
        i.second.Stop();
    }
    return *this;
}

const NTiers::TManager* TTiersManager::GetManagerOptional(const TString& tierId) const {
    auto it = Managers.find(tierId);
    if (it != Managers.end()) {
        return &it->second;
    } else {
        return nullptr;
    }
}

void TTiersManager::EnablePathId(const ui64 pathId, const THashSet<TString>& usedTiers) {
    AFL_VERIFY(Actor)("error", "tiers_manager_is_not_started");
    auto& tierRefs = UsedTiers[pathId];
    tierRefs.clear();
    for (const TString& tierName : usedTiers) {
        tierRefs.emplace_back(tierName, TierRefCount);
        if (!Tiers.contains(tierName)) {
            const auto& actorContext = NActors::TActivationContext::AsActorContext();
            AFL_VERIFY(&actorContext)("error", "no_actor_context");
            actorContext.Send(Actor->SelfId(), new NTiers::TEvWatchTieredStorages({tierName}));
        }
    }
    OnConfigsUpdated(false);
}

void TTiersManager::DisablePathId(const ui64 pathId) {
    UsedTiers.erase(pathId);
    OnConfigsUpdated(false);
}

void TTiersManager::UpdateSecretsSnapshot(std::shared_ptr<NMetadata::NSecret::TSnapshot> secrets) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_secrets")("tablet", TabletId);
    AFL_VERIFY(secrets);
    Secrets = secrets;
    OnConfigsUpdated();
}

void TTiersManager::UpdateTierConfig(const NTiers::TTierConfig& config, const TString& tierName, const bool notifyShard) {
    AFL_INFO(NKikimrServices::TX_TIERING)("event", "update_tier_config")("name", tierName)("tablet", TabletId)("notify_shard", notifyShard);
    Tiers[tierName] = config;
    OnConfigsUpdated(notifyShard);
}

TActorId TTiersManager::GetActorId() const {
    if (Actor) {
        return Actor->SelfId();
    } else {
        return {};
    }
}

TString TTiersManager::DebugString() {
    TStringBuilder sb;
    sb << "TIERS=";
    if (Tiers) {
        sb << "{";
        for (const auto& [name, config] : Tiers) {
            sb << name << ";";
        }
        sb << "}";
    }
    sb << ";USED_TIERS=";
    {
        sb << "{";
        for (const auto& [pathId, tiers] : UsedTiers) {
            sb << pathId << ":{";
            for (const auto& tierRef : tiers) {
                sb << tierRef.GetTierName() << ";";
            }
            sb << "}";
        }
        sb << "}";
    }
    sb << ";SECRETS=";
    if (Secrets) {
        sb << "{";
        for (const auto& [name, config] : Secrets->GetSecrets()) {
            sb << name.SerializeToString() << ";";
        }
        sb << "}";
    }
    return sb;
}
}
