#include "list.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/common/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

// TODO: Must be a base class with specification for TieredStorage
class TFetchTieredStoragesActor: public NKqp::NWorkload::TSchemeActorBase<TFetchTieredStoragesActor> {
private:
    TActorId Recipient;
    std::set<TPathId> UnfetchedObjects;
    THashMap<TString, TTierConfig> Result;

private:
    void ReplyErrorAndPassAway(const TString& errorMessage) {
        Send(Recipient, new NTiers::TEvListTieredStoragesResult(TConclusionStatus::Fail(errorMessage)));
        PassAway();
    }

    void ReplySuccessAndPassAway() {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_lister")("event", "send_tiering_rules")("size", Result.size());
        Send(Recipient, new NTiers::TEvListTieredStoragesResult(std::move(Result)));
        PassAway();
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> BuildFetchRequest(const std::set<TPathId>& paths) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(NACLib::TSystemUsers::Metadata());

        for (const auto& pathId : paths) {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
            entry.TableId.PathId = pathId;
            entry.ShowPrivatePath = true;
        }

        return request;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        for (const auto& result : results) {
            switch (result.Status) {
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::AccessDenied:
                    AFL_VERIFY(false)("status", result.Status)("result", result.ToString());
                case EStatus::Unknown:
                case EStatus::RedirectLookupError:
                case EStatus::LookupError:
                case EStatus::TableCreationNotComplete:
                    if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                        ReplyErrorAndPassAway("Retry limit exceeded");
                        return;
                    }
                    break;
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                    OnObjectFetched(std::nullopt, result.TableId.PathId);
                    break;
                case EStatus::Ok:
                    AFL_VERIFY(result.Kind == NSchemeCache::TSchemeCacheNavigate::KindTieredStorage)("kind", result.Kind)("result", result.ToString());
                    OnObjectFetched(result.TieredStorageInfo->Description, result.TableId.PathId);
                    break;
            }
        }

        if (UnfetchedObjects.empty()) {
            ReplySuccessAndPassAway();
            return;
        }
    }

    void OnObjectFetched(std::optional<NKikimrSchemeOp::TMetadataObjectDescription> description, const TPathId& pathId) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_lister")("event", "object_fetched")("exists", !!description);
        if (description) {
            TTierConfig tieredStorage;
            AFL_VERIFY(tieredStorage.DeserializeFromProto(description->GetProperties()));
            Result.emplace(description->GetName(), std::move(tieredStorage));
        }
        UnfetchedObjects.erase(pathId);
    }

protected:
    void StartRequest() override {
        auto event = BuildFetchRequest(UnfetchedObjects);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode /*status*/, NYql::TIssue issue) override {
        ReplyErrorAndPassAway(issue.ToString(true));
    }

    TString LogPrefix() const override {
        return "[TFetchTieredStoragesActor] ";
    }

public:
    TFetchTieredStoragesActor(const TActorId& recipient, std::set<TPathId> tieredStorages)
        : Recipient(recipient)
        , UnfetchedObjects(std::move(tieredStorages)) {
    }

    void DoBootstrap() {
        Become(&TFetchTieredStoragesActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }
};

class TListTieredStoragesActor: public NKqp::NWorkload::TSchemeActorBase<TListTieredStoragesActor> {
private:
    TActorId Recipient;

private:
    void ReplyErrorAndPassAway(const TString& errorMessage) {
        Send(Recipient, new NTiers::TEvListTieredStoragesResult(TConclusionStatus::Fail(errorMessage)));
        PassAway();
    }

    static THolder<NSchemeCache::TSchemeCacheNavigate> BuildListRequest(const TVector<TString>& pathComponents) {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppDataVerified().TenantName;
        request->UserToken = MakeIntrusive<NACLib::TUserToken>(NACLib::TSystemUsers::Metadata());

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.Path = pathComponents;

        return request;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        AFL_VERIFY(results.size() == 1)("size", results.size());
        const auto& result = results[0];
        switch (result.Status) {
            case EStatus::AccessDenied:
            case EStatus::PathNotTable:
            case EStatus::PathNotPath:
                AFL_VERIFY(false)("status", result.Status)("result", result.ToString());
                return;
            case EStatus::Unknown:
            case EStatus::RedirectLookupError:
            case EStatus::LookupError:
            case EStatus::TableCreationNotComplete:
                if (!ScheduleRetry(TStringBuilder() << "Retry error " << result.Status)) {
                    ReplyErrorAndPassAway("Retry limit exceeded");
                }
                return;
            case EStatus::RootUnknown:
            case EStatus::PathErrorUnknown:
                AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_lister")("event", "unknown_path")("path", JoinPath(result.Path));
                OnObjectsListed({});
                return;
            case EStatus::Ok:
                OnObjectsListed(result.ListNodeEntry->Children);
                return;
        }
    }

    void OnObjectsListed(TVector<NSchemeCache::TSchemeCacheNavigate::TListNodeEntry::TChild> nodes) {
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_lister")("event", "objects_listed")("size", nodes.size());
        std::set<TPathId> objects;
        for (const auto& node : nodes) {
            objects.insert(node.PathId);
        }
        TActivationContext::Register(new TFetchTieredStoragesActor(Recipient, std::move(objects)), Recipient);
        PassAway();
    }

protected:
    void StartRequest() override {
        const TString storagePath = NTiers::TTierConfig::GetBehaviour()->GetStorageTablePath();
        AFL_DEBUG(NKikimrServices::TX_TIERING)("component", "tiering_lister")("event", "send_list_request")("path", storagePath);
        auto event = BuildListRequest(SplitPath(storagePath));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void OnFatalError(Ydb::StatusIds::StatusCode /*status*/, NYql::TIssue issue) override {
        ReplyErrorAndPassAway(issue.ToString(true));
    }

    TString LogPrefix() const override {
        return "[TListTieredStoragesActor] ";
    }

public:
    TListTieredStoragesActor(const TActorId recipient)
        : Recipient(recipient) {
    }

    void DoBootstrap() {
        Become(&TListTieredStoragesActor::StateMain);
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                StateFuncBase(ev);
        }
    }
};

}   // namespace NTiers

THolder<IActor> MakeListTieredStoragesActor(TActorId recipient) {
    return MakeHolder<NTiers::TListTieredStoragesActor>(recipient);
}

}   // namespace NKikimr::NColumnShard
