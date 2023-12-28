#include "datashard_write_operation.h"
#include "datashard_pipeline.h"
#include "setup_sys_locks.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"

#include <ydb/core/engine/mkql_engine_flat_host.h>

namespace NKikimr {
namespace NDataShard {

class TWriteUnit : public TExecutionUnit {
public:
    TWriteUnit(TDataShard& self, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::ExecuteWrite, true, self, pipeline)
    {
    }

    ~TWriteUnit()
    {
    }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (op->HasRuntimeConflicts() || op->HasWaitingForGlobalTxIdFlag()) {
            return false;
        }

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && WillRejectDataTx(op)) {
            return true;
        }

        if (DataShard.IsStopping()) {
            // Avoid doing any new work when datashard is stopping
            return false;
        }

        return !op->HasRuntimeConflicts();
    }

    void DoExecute(TDataShard* self, TWriteOperation* writeOp, TTransactionContext& txc, const TActorContext& ctx) {
        TValidatedWriteTx::TPtr& writeTx = writeOp->GetWriteTx();

        const ui64 tableId = writeTx->GetTableId().PathId.LocalPathId;
        const TTableId fullTableId(self->GetPathOwnerId(), tableId);
        const ui64 localTableId = self->GetLocalTableId(fullTableId);
        if (localTableId == 0) {
            writeOp->SetError(NKikimrDataEvents::TEvWriteResult::STATUS_INTERNAL_ERROR, TStringBuilder() << "Unknown table id " << tableId, self->TabletID());
            return;
        }
        const ui64 shadowTableId = self->GetShadowTableId(fullTableId);

        const TUserTable& TableInfo_ = *self->GetUserTables().at(tableId);
        Y_ABORT_UNLESS(TableInfo_.LocalTid == localTableId);
        Y_ABORT_UNLESS(TableInfo_.ShadowTid == shadowTableId);

        auto [readVersion, writeVersion] = self->GetReadWriteVersions(writeOp);
        writeTx->SetReadVersion(readVersion);
        writeTx->SetWriteVersion(writeVersion);

        TDataShardUserDb userDb(*self, txc.DB, readVersion);
        TDataShardChangeGroupProvider groupProvider(*self, txc.DB);

        TVector<TCell> keyCells;
        TVector<NMiniKQL::IEngineFlatHost::TUpdateCommand> commands;

        const TSerializedCellMatrix& matrix = writeTx->GetMatrix();

        for (ui32 rowIdx = 0; rowIdx < matrix.GetRowCount(); ++rowIdx)
        {
            keyCells.clear();
            keyCells.reserve(TableInfo_.KeyColumnIds.size());
            ui64 keyBytes = 0;
            for (ui16 keyColIdx = 0; keyColIdx < TableInfo_.KeyColumnIds.size(); ++keyColIdx) {
                const TCell& cell = matrix.GetCell(rowIdx, keyColIdx);
                keyBytes += cell.IsNull() ? 1 : cell.Size();
                keyCells.emplace_back(cell);
            }

            commands.clear();
            Y_ABORT_UNLESS(matrix.GetColCount() >= TableInfo_.KeyColumnIds.size());
            commands.reserve(matrix.GetColCount() - TableInfo_.KeyColumnIds.size());

            ui64 valueBytes = 0;
            for (ui16 valueColIdx = TableInfo_.KeyColumnIds.size(); valueColIdx < matrix.GetColCount(); ++valueColIdx) {
                ui32 columnTag = writeTx->RecordOperation().GetColumnIds(valueColIdx);
                const TCell& cell = matrix.GetCell(rowIdx, valueColIdx);
                valueBytes += cell.IsNull() ? 1 : cell.Size();

                NMiniKQL::IEngineFlatHost::TUpdateCommand command = {columnTag, TKeyDesc::EColumnOperation::Set, {}, cell};
                commands.emplace_back(std::move(command));
            }

            writeTx->GetEngineHost()->UpdateRow(fullTableId, keyCells, commands);
        }
        //TODO: Counters
        // self->IncCounter(COUNTER_UPLOAD_ROWS, rowCount);
        // self->IncCounter(COUNTER_UPLOAD_ROWS_BYTES, matrix.GetBuffer().size());

        writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildCommited(self->TabletID(), writeOp->GetTxId()));

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executed write operation for " << *writeOp << " at " << self->TabletID());
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        TWriteOperation* writeOp = dynamic_cast<TWriteOperation*>(op.Get());
        Y_ABORT_UNLESS(writeOp != nullptr);

        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Executing write operation for " << *op << " at " << DataShard.TabletID());

        if (op->Result() || op->HasResultSentFlag() || op->IsImmediate() && CheckRejectDataTx(op, ctx)) {
            return EExecutionStatus::Executed;
        }

        if (op->HasWaitingForGlobalTxIdFlag()) {
            return EExecutionStatus::Continue;
        }

        if (op->IsImmediate()) {
            // Every time we execute immediate transaction we may choose a new mvcc version
            op->MvccReadWriteVersion.reset();
        }
        else {
            //TODO: Prepared
            writeOp->SetWriteResult(NEvents::TDataEvents::TEvWriteResult::BuildPrepared(DataShard.TabletID(), op->GetTxId(), {0, 0, {}}));
            return EExecutionStatus::DelayCompleteNoMoreRestarts;
        }

        TDataShardLocksDb locksDb(DataShard, txc);
        TSetupSysLocks guardLocks(op, DataShard, &locksDb);

        try {
            DoExecute(&DataShard, writeOp, txc, ctx);
        } catch (const TNeedGlobalTxId&) {
            Y_VERIFY_S(op->GetGlobalTxId() == 0,
                "Unexpected TNeedGlobalTxId exception for write operation with TxId# " << op->GetGlobalTxId());
            Y_VERIFY_S(op->IsImmediate(),
                "Unexpected TNeedGlobalTxId exception for a non-immediate write operation with TxId# " << op->GetTxId());

            ctx.Send(MakeTxProxyID(),
                new TEvTxUserProxy::TEvAllocateTxId(),
                0, op->GetTxId());
            op->SetWaitingForGlobalTxIdFlag();

            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        if (Pipeline.AddLockDependencies(op, guardLocks)) {
            if (txc.DB.HasChanges()) {
                txc.DB.RollbackChanges();
            }
            return EExecutionStatus::Continue;
        }

        op->ChangeRecords() = std::move(writeOp->GetWriteTx()->GetCollectedChanges());

        DataShard.SysLocksTable().ApplyLocks();
        DataShard.SubscribeNewLocks(ctx);
        Pipeline.AddCommittingOp(op);

        return EExecutionStatus::DelayCompleteNoMoreRestarts;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Pipeline.RemoveCommittingOp(op);
        DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
        DataShard.EmitHeartbeats(ctx);

        TWriteOperation* writeOp = dynamic_cast<TWriteOperation*>(op.Get());
        Y_ABORT_UNLESS(writeOp != nullptr);

        const auto& status = writeOp->GetWriteResult()->Record.status();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_DATASHARD, "Completed write operation for " << *op << " at " << DataShard.TabletID() << ", status " << status);

        //TODO: Counters
        // if (WriteResult->Record.status() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED || WriteResult->Record.status() == NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED) {
        //     self->IncCounter(COUNTER_WRITE_SUCCESS);
        // } else {
        //     self->IncCounter(COUNTER_WRITE_ERROR);
        // }

        ctx.Send(writeOp->GetEv()->Sender, writeOp->ReleaseWriteResult().release(), 0, writeOp->GetEv()->Cookie);
    }

};  // TWriteUnit

THolder<TExecutionUnit> CreateWriteUnit(TDataShard& self, TPipeline& pipeline) {
    return THolder(new TWriteUnit(self, pipeline));
}

} // NDataShard
} // NKikimr