/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.*;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.HsqlNameManager.HsqlName;

import java.util.HashMap;

/**
 * Manages rows involved in transactions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.0.0
 */
public class TransactionManager2PLRow extends TransactionManagerCommon
implements TransactionManager {
    // functional unit - merged committed transactions
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();

    // locks
    boolean isLockedMode;
    Session catalogWriteSession;

    //
    long lockTxTs;
    long lockSessionId;
    long unlockTxTs;
    long unlockSessionId;
    //row
    org.hsqldb.lib.HashMap RowWriteLocks = new org.hsqldb.lib.HashMap();
    MultiValueHashMap RowReadLocks  = new MultiValueHashMap();

    //
    int redoCount = 0;

    public TransactionManager2PLRow(Database db) {

        database   = db;
        lobSession = database.sessionManager.getSysLobSession();
        txModel    = ROWLOCKS;
        rowActionMap = new LongKeyHashMap(10000);
    }

    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }

    public boolean isMVRows() {
        return false;
    }

    public boolean isMVCC() {
        return false;
    }
    //row
    public boolean isRowLocks() {return true; }

    public int getTransactionControl() {
        return ROWLOCKS;
    }

    public void setTransactionControl(Session session, int mode) {
        super.setTransactionControl(session, mode);
    }

    public void completeActions(Session session) {
        //endActionTPL(session);
    }

    public boolean prepareCommitActions(Session session) {

        session.actionTimestamp = getNextGlobalChangeTimestamp();

        return true;
    }

    public boolean commitTransaction(Session session) {

        if (session.abortTransaction) {
            return false;
        }

        writeLock.lock();

        try {
            int limit = session.rowActionList.size();

            // new actionTimestamp used for commitTimestamp
            session.actionTimestamp         = getNextGlobalChangeTimestamp();
            session.transactionEndTimestamp = session.actionTimestamp;

            endTransaction(session);

            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) session.rowActionList.get(i);

                action.commit(session);
            }

            adjustLobUsage(session);
            persistCommit(session);
            //row
            if(session.isolationLevel == SessionInterface.TX_SERIALIZABLE){
                endTransactionTPL(session);
            }
            else {
                endTransactionTPLRow(session);
            }
        } finally {
            writeLock.unlock();
        }

        session.tempSet.clear();

        return true;
    }

    public void rollback(Session session) {

        session.abortTransaction        = false;
        session.actionTimestamp         = getNextGlobalChangeTimestamp();
        session.transactionEndTimestamp = session.actionTimestamp;

        rollbackPartial(session, 0, session.transactionTimestamp);
        endTransaction(session);
        writeLock.lock();

        try {
            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }
    }

    public void rollbackSavepoint(Session session, int index) {

        long timestamp = session.sessionContext.savepointTimestamps.get(index);
        Integer oi = (Integer) session.sessionContext.savepoints.get(index);
        int     start  = oi.intValue();

        while (session.sessionContext.savepoints.size() > index + 1) {
            session.sessionContext.savepoints.remove(
                session.sessionContext.savepoints.size() - 1);
            session.sessionContext.savepointTimestamps.removeLast();
        }

        rollbackPartial(session, start, timestamp);
    }

    public void rollbackAction(Session session) {

        rollbackPartial(session, session.actionIndex,
                        session.actionStartTimestamp);
        endActionTPL(session);
    }

    /**
     * rollback the row actions from start index in list and
     * the given timestamp
     */
    public void rollbackPartial(Session session, int start, long timestamp) {

        int limit = session.rowActionList.size();

        if (start == limit) {
            return;
        }

        for (int i = limit - 1; i >= start; i--) {
            RowAction action = (RowAction) session.rowActionList.get(i);

            if (action == null || action.type == RowActionBase.ACTION_NONE
                    || action.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }

            Row row = action.memoryRow;

            if (row == null) {
                row = (Row) action.store.get(action.getPos(), false);
            }

            if (row == null) {
                continue;
            }

            action.rollback(session, timestamp);

            int type = action.mergeRollback(session, timestamp, row);

            action.store.rollbackRow(session, row, type, txModel);
        }

        session.rowActionList.setSize(start);
    }

    public RowAction addDeleteAction(Session session, Table table,
                                     PersistentStore store, Row row,
                                     int[] colMap) {

        RowAction action;

        synchronized (row) {
            action = RowAction.addDeleteAction(session, table, row, colMap);
        }

        session.rowActionList.add(action);
        store.delete(session, row);

        row.rowAction = null;

        return action;
    }

    public void addInsertAction(Session session, Table table,
                                PersistentStore store, Row row,
                                int[] changedColumns) {

        RowAction action = row.rowAction;

        if (action == null) {
/*
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
*/
            throw Error.runtimeError(ErrorCode.GENERAL_ERROR,
                    "null insert action ");
        }

        store.indexRow(session, row);
        session.rowActionList.add(action);

        row.rowAction = null;
    }

// functional unit - accessibility of rows
    public boolean canRead(Session session, PersistentStore store, Row row,
                           int mode, int[] colMap) {
        return true;
    }

    public boolean canRead(Session session, PersistentStore store, long id,
                           int mode) {
        return true;
    }

    public void addTransactionInfo(CachedObject object) {}

    /**
     * add transaction info to a row just loaded from the cache. called only
     * for CACHED tables
     */
    public void setTransactionInfo(PersistentStore store,
                                   CachedObject object) {}

    public void removeTransactionInfo(CachedObject object) {}

    public void beginTransaction(Session session) {

        if (!session.isTransaction) {
            session.actionTimestamp      = getNextGlobalChangeTimestamp();
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;

            transactionCount++;
        }
    }

    /**
     * add session to the end of queue when a transaction starts
     * (depending on isolation mode)
     */
    public void beginAction(Session session, Statement cs) {

        //IF serializable, lock become table lock
        if (session.isolationLevel != SessionInterface.TX_SERIALIZABLE) {
            return;
        }

        writeLock.lock();

        try {
            if (cs.getCompileTimestamp()
                    < database.schemaManager.getSchemaChangeTimestamp()) {
                cs = session.statementManager.getStatement(session, cs);
                session.sessionContext.currentStatement = cs;

                if (cs == null) {
                    return;
                }
            }

            boolean canProceed = setWaitedSessionsTPL(session, cs);

            if (canProceed) {
                if (session.tempSet.isEmpty()) {
                    lockTablesTPL(session, cs);

                    // we don't set other sessions that would now be waiting for this one too
                    // next lock release will do it
                } else {
                    setWaitingSessionTPL(session);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void beginActionRow(Session session, long rowId, Statement cs, int statementType) {

        if (session.hasLocks(cs)) {
            return;
        }

        writeLock.lock();

        try {
            if (cs.getCompileTimestamp()
                    < database.schemaManager.getSchemaChangeTimestamp()) {
                cs = session.statementManager.getStatement(session, cs);
                session.sessionContext.currentStatement = cs;

                if (cs == null) {
                    return;
                }
            }

            boolean canProceed = setWaitedSessionsTPLRow(session, rowId, cs, statementType);

            if (canProceed) {
                if (session.tempSet.isEmpty()) {
                    lockRowTPL(session, rowId, cs, statementType);

                    // we don't set other sessions that would now be waiting for this one too
                    // next lock release will do it
                } else {
                    setWaitingSessionTPLRow(session, rowId, statementType);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    void lockRowTPL(Session session, long rowId, Statement cs, int statementType) {

        if (cs == null || session.abortTransaction) {
            return;
        }

        //0 read 1 write
        if(statementType == 0){
            RowReadLocks.put(rowId, session);
        }
        else {
            RowWriteLocks.put(rowId, session);
        }
    }

    boolean setWaitedSessionsTPLRow(Session session, long rowId, Statement cs, int statementType) {

        session.tempSet.clear();

        if (cs == null) {
            return true;
        }

        if (session.abortTransaction) {
            return false;
        }
        //这里只考虑RU RC RR
        //read locks
        if(statementType == 0){
            //get read locks
            //no need to care about other read locks
            if(session.isolationLevel != SessionInterface.TX_READ_UNCOMMITTED) {
                Session holder = (Session) RowWriteLocks.get(rowId);
                if (holder != null && holder != session) {
                    session.tempSet.add(holder);
                }
            }
        }
        //写
        else{
            Session holder = (Session) RowWriteLocks.get(rowId);
            if (holder != null && holder != session) {
                session.tempSet.add(holder);
            }
            Iterator it = RowReadLocks.get(rowId);

            while (it.hasNext()) {
                holder = (Session) it.next();

                if (holder != session) {
                    session.tempSet.add(holder);
                }
            }
        }

        if (session.tempSet.isEmpty()) {
            return true;
        }

        if (checkDeadlock(session, session.tempSet)) {
            return true;
        }

        session.tempSet.clear();

        session.abortTransaction = true;

        return false;
    }


    public void beginActionResume(Session session) {

        session.actionTimestamp      = getNextGlobalChangeTimestamp();
        session.actionStartTimestamp = session.actionTimestamp;

        if (!session.isTransaction) {
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;

            transactionCount++;
        }

        return;
    }

    public void removeTransactionInfo(long id) {}

    void endTransaction(Session session) {

        if (session.isTransaction) {
            session.isTransaction = false;

            transactionCount--;
        }
    }

    void endActionTPLRow(Session session, long rowId, int statementType) {

        if (session.isolationLevel == SessionInterface.TX_REPEATABLE_READ) {
            return;
        }

        if (session.sessionContext.currentStatement == null) {

            // after java function / proc with db access
            return;
        }

        if (session.sessionContext.depth > 0) {

            // routine or trigger
            return;
        }

//        if (readLocks.length == 0) {
//            return;
//        }

        writeLock.lock();

        try {
            if(session.isolationLevel == SessionInterface.TX_READ_UNCOMMITTED ||
                    session.isolationLevel == SessionInterface.TX_READ_COMMITTED) {
                unlockReadRowTPL(session, rowId);
            }

            final int waitingCount = session.waitingSessions.size();

            if (waitingCount == 0) {
                return;
            }

            boolean canUnlock = false;

            // if write lock was used for read lock

            if (RowWriteLocks.get(rowId) != session) {
                canUnlock = true;
            }


            if (!canUnlock) {
                return;
            }

            canUnlock = false;

            for (int i = 0; i < waitingCount; i++) {
                Session current = (Session) session.waitingSessions.get(i);

                if (current.abortTransaction) {
                    canUnlock = true;

                    break;
                }

                Statement currentStatement =
                        current.sessionContext.currentStatement;

                if (currentStatement == null) {
                    canUnlock = true;

                    break;
                }
            }

            if (!canUnlock) {
                return;
            }

            resetLocksRow(session);
            resetLatchesMidTransactionRow(session);
        } finally {
            writeLock.unlock();
        }
    }

    void endTransactionTPLRow(Session session) {

        unlockRowTPL(session);

        final int waitingCount = session.waitingSessions.size();

        if (waitingCount == 0) {
            return;
        }

        resetLocksRow(session);
        resetLatchesRow(session);
    }

    void resetLocksRow(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);
            long rowId = (long) session.waitingSessionsRow.get(i);
            int rowType = (int) session.waitingSessionsRowType.get(i);

            current.tempUnlocked = false;

            long count = current.latch.getCount();

            if (count == 1) {
                boolean canProceed = setWaitedSessionsTPLRow(current,rowId,
                        current.sessionContext.currentStatement, rowType);

                if (canProceed) {
                    if (current.tempSet.isEmpty()) {
                        lockRowTPL(current, rowId,
                                current.sessionContext.currentStatement, rowType);

                        current.tempUnlocked = true;
                    }
                }
            }
        }

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);
            long rowId = (long) session.waitingSessionsRow.get(i);
            int rowType = (int) session.waitingSessionsRowType.get(i);

            if (current.tempUnlocked) {

                //
            } else if (current.abortTransaction) {

                //
            } else {

                // this can introduce additional waits for the sessions
                setWaitedSessionsTPLRow(current, rowId,
                        current.sessionContext.currentStatement, rowType);
            }
        }
    }

    void resetLatchesRow(Session session) {

        final int waitingCount = session.waitingSessions.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.waitingSessions.get(i);
            long rowId = (long) session.waitingSessionsRow.get(i);
            int rowType = (int) session.waitingSessionsRowType.get(i);
/*
            if (!current.abortTransaction && current.tempSet.isEmpty()) {


                // test code valid only for top level statements
                boolean hasLocks = hasLocks(current, current.sessionContext.currentStatement);
                if (!hasLocks) {
                    System.out.println("trouble");
                }

            }
*/
            setWaitingSessionTPLRow(current, rowId, rowType);
        }

        session.waitingSessions.clear();
        session.waitingSessionsRow.clear();
        session.waitingSessionsRowType.clear();

        session.latch.setCount(0);
    }

    void resetLatchesMidTransactionRow(Session session) {

        session.tempSet.clear();
        session.tempSet.addAll(session.waitingSessions);
        OrderedHashSet tempRow = new OrderedHashSet();
        tempRow.addAll(session.waitingSessionsRow);
        OrderedHashSet tempRowType = new OrderedHashSet();
        tempRowType.addAll(session.waitingSessionsRowType);

        session.waitingSessions.clear();
        session.waitingSessionsRow.clear();
        session.waitingSessionsRowType.clear();

        final int waitingCount = session.tempSet.size();

        for (int i = 0; i < waitingCount; i++) {
            Session current = (Session) session.tempSet.get(i);
            //row
            long rowId = (long) tempRow.get(i);
            int rowType = (int) tempRowType.get(i);


            if (!current.abortTransaction && current.tempSet.isEmpty()) {

                // valid for top level statements
//                boolean hasLocks = hasLocks(current, current.sessionContext.currentStatement);
//                if (!hasLocks) {
//                    System.out.println("trouble");
//                    hasLocks(current, current.sessionContext.currentStatement);
//                }
            }

            setWaitingSessionTPLRow(current, rowId, rowType);
        }

        session.tempSet.clear();
    }

    void unlockRowTPL(Session session) {

        Iterator it = RowWriteLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.remove();
            }
        }

        it = RowReadLocks.values().iterator();

        while (it.hasNext()) {
            Session s = (Session) it.next();

            if (s == session) {
                it.remove();
            }
        }
    }

    void unlockReadRowTPL(Session session, long rowId) {
        RowReadLocks.remove(rowId, session);
    }

    void setWaitingSessionTPLRow(Session session, long rowId, int statementType) {

        int count = session.tempSet.size();

        assert session.latch.getCount() <= count + 1;

        for (int i = 0; i < count; i++) {
            Session current = (Session) session.tempSet.get(i);

            current.waitingSessions.add(session);
            current.waitingSessionsRow.add(rowId);
            current.waitingSessionsRowType.add(statementType);
        }

        session.tempSet.clear();
        session.latch.setCount(count);
    }

}
