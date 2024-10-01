#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tiering/tier/object.h>
#include <ydb/core/tx/tiering/rule/object.h>

#include <ydb/services/metadata/service.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NColumnShard::NTiers {

class TConfigsSnapshot {
private:
    using TConfigsMap = TMap<TString, TTierConfig>;
    YDB_ACCESSOR_DEF(TConfigsMap, TierConfigs);
    using TTieringMap = TMap<TString, TTieringRule>;
    YDB_ACCESSOR_DEF(TTieringMap, TableTierings);
public:

    std::set<TString> GetTieringIdsForTier(const TString& tierName) const;
    const TTieringRule* GetTieringById(const TString& tieringId) const;
    std::optional<TTierConfig> GetTierById(const TString& tierName) const;
};

}
