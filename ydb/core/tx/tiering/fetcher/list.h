#pragma once

#include <ydb/core/tx/tiering/common.h>
#include <ydb/core/tx/tiering/tier/object.h>

#include <ydb/library/conclusion/result.h>

#include <library/cpp/threading/future/core/future.h>

namespace NKikimr::NColumnShard {

namespace NTiers {

class TEvListTieredStoragesResult: public TEventLocal<TEvListTieredStoragesResult, EvListTieredStoragesResult> {
private:
    using TTieredStorages = THashMap<TString, TTierConfig>;
    TConclusion<TTieredStorages> Result;

public:
    TEvListTieredStoragesResult(TConclusion<TTieredStorages> result)
        : Result(std::move(result)) {
    }

    const TConclusion<TTieredStorages>& GetResult() const {
        return Result;
    }
};

}   // namespace NTiers

THolder<IActor> MakeListTieredStoragesActor(TActorId recipient);

}   // namespace NKikimr::NColumnShard
