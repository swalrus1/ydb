#pragma once
#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor_virtual.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/metadata/abstract/kqp_common.h>

#include <library/cpp/object_factory/object_factory.h>
#include <util/system/type_name.h>

namespace NKikimr::NMetadata::NFetcher {

class ISnapshot;

class ISnapshotAcceptorController {
public:
    using TPtr = std::shared_ptr<ISnapshotAcceptorController>;
    virtual ~ISnapshotAcceptorController() = default;
    virtual void OnSnapshotEnriched(std::shared_ptr<ISnapshot> enrichedSnapshot) = 0;
    virtual void OnSnapshotEnrichError(const TString& errorMessage) = 0;
};

class ISnapshot {
private:
    YDB_ACCESSOR_DEF(TInstant, Actuality);
protected:
    virtual bool DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) = 0;
    virtual TString DoSerializeToString() const = 0;

    template <class TObject, class TActor>
    bool ParseSnapshotObjects(const Ydb::ResultSet& rawData, const TActor& actor, const bool stopOnIncorrectDeserialization = false) {
        typename TObject::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TObject object;
            if (!object.DeserializeFromRecord(decoder, r)) {
                ALS_WARN(NKikimrServices::METADATA_PROVIDER) << "cannot parse object: " << TypeName<TObject>();
                if (stopOnIncorrectDeserialization) {
                    return false;
                } else {
                    continue;
                }
            }
            actor(std::move(object));
        }
        return true;
    }
public:
    using TPtr = std::shared_ptr<ISnapshot>;
    ISnapshot(const TInstant actuality)
        : Actuality(actuality) {

    }

    bool DeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) {
        return DoDeserializeFromResultSet(rawData);
    }

    TString SerializeToString() const {
        return DoSerializeToString();
    }

    virtual ~ISnapshot() = default;
};

class ISnapshotsFetcher {
private:
    mutable std::vector<IClassBehaviour::TPtr> Managers;
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const = 0;
    virtual std::vector<IClassBehaviour::TPtr> DoGetManagers() const = 0;
public:
    using TPtr = std::shared_ptr<ISnapshotsFetcher>;

    ISnapshot::TPtr CreateEmpty(const TInstant actuality) const {
        return CreateSnapshot(actuality);
    }

    virtual TString GetComponentId() const;
    ISnapshot::TPtr ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const;

    virtual void EnrichSnapshotData(ISnapshot::TPtr original, ISnapshotAcceptorController::TPtr controller) const {
        controller->OnSnapshotEnriched(original);
    }

    const std::vector<IClassBehaviour::TPtr>& GetManagers() const {
        if (Managers.empty()) {
            Managers = DoGetManagers();
        }
        return Managers;
    }

    virtual ~ISnapshotsFetcher() = default;
};

template <class TSnapshot>
class TSnapshotsFetcher: public ISnapshotsFetcher {
protected:
    virtual ISnapshot::TPtr CreateSnapshot(const TInstant actuality) const override {
        return std::make_shared<TSnapshot>(actuality);
    }
};

}
