#ifndef _TXN_PROCESSOR_H_
#define _TXN_PROCESSOR_H_

#include <deque>
#include <map>
#include <string>

#include "lock_manager.h"
#include "mvcc_storage.h"
#include "storage.h"
#include "txn.h"
#include "utils/atomic.h"
#include "utils/common.h"
#include "utils/mutex.h"
#include "utils/static_thread_pool.h"

using std::deque;
using std::map;
using std::string;

// The TxnProcessor supports five different execution modes, corresponding to
// the four parts of assignment 2, plus a simple serial (non-concurrent) mode.
enum CCMode
{
    SERIAL                 = 0,  // Serial transaction execution (no concurrency)
    LOCKING_EXCLUSIVE_ONLY = 1,  // Part 1A
    LOCKING                = 2,  // Part 1B
    OCC_SERIAL_FORWARD_VALIDATION = 3,
    OCC_SERIAL_BACKWARD_VALIDATION = 4,
    OCC_PARREL_FORWARD_VALIDATION = 5,
    OCC_PARREL_BACKWARD_VALIDATION = 6,
    MVCC_MVTO                   = 7,
    MVCC_MV2PL                   = 8,  
};

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode);

class TxnProcessor
{
   public:
    // The TxnProcessor's constructor starts the TxnProcessor running in the
    // background.
    explicit TxnProcessor(CCMode mode);

    // The TxnProcessor's destructor stops all background threads and deallocates
    // all objects currently owned by the TxnProcessor, except for Txn objects.
    ~TxnProcessor();

    // Registers a new txn request to be executed by the TxnProcessor.
    // Ownership of '*txn' is transfered to the TxnProcessor.
    void NewTxnRequest(Txn* txn);

    // Returns a pointer to the next COMMITTED or ABORTED Txn. The caller takes
    // ownership of the returned Txn.
    Txn* GetTxnResult();

    vector<Txn*> GetTxnVectorResults();

    // Main loop implementing all concurrency control/thread scheduling.
    void RunScheduler();

    static void* StartScheduler(void* arg);

    
   private:
    // Serial validation
    bool SerialValidate(Txn* txn);

    // Parallel execution/validation for OCC
    void ExecuteTxnParallelBackwardValidation(Txn* txn);


    // Parallel execution/validation for OCC
    void ExecuteTxnParallelForwardValidation(Txn* txn);

    // Serial version of scheduler.
    void RunSerialScheduler();

    // Locking version of scheduler.
    void RunLockingScheduler();

   

    //OCC serial version of scheduler forward validation.
    void RunOCCSerialSchedulerForwardValidation();

    //OCC serial version of scheduler backward validation.
    void RunOCCSerialSchedulerBackwardValidation();


    //OCC parallel version of scheduler forward validation.
    void RunOCCParallelSchedulerForwardValidation();

    //OCC parallel version of scheduler backward validation.
    void RunOCCParallelSchedulerBackwardValidation();


    // MVCC version of scheduler.
    void RunMVCCMVTOScheduler();

    // Performs all reads required to execute the transaction, then executes the
    // transaction logic.
    void ExecuteTxn(Txn* txn);


    bool MVCC2PLCheckWrites(Txn* txn);

    void RunMVCCMV2PLScheduler();

    void MVCC2PLExecuteTxn(Txn* txn);


    // Applies all writes performed by '*txn' to 'storage_'.
    //
    // Requires: txn->Status() is COMPLETED_C.
    void ApplyWrites(Txn* txn);

    // The following functions are for MVCC
    void MVCCMVTOExecuteTxn(Txn* txn);

    bool MVCCCheckWrites(Txn* txn);

    void MVCCLockWriteKeys(Txn* txn);

    void MVCCUnlockWriteKeys(Txn* txn);

    void GarbageCollection();

    // Concurrency control mechanism the TxnProcessor is currently using.
    CCMode mode_;

    // Thread pool managing all threads used by TxnProcessor.
    StaticThreadPool tp_;

    // Data storage used for all modes.
    Storage* storage_;

    // Next valid unique_id, and a mutex to guard incoming txn requests.
    int next_unique_id_;
    Mutex mutex_;

    // Queue of incoming transaction requests.
    AtomicQueue<Txn*> txn_requests_;

    // Queue of txns that have acquired all locks and are ready to be executed.
    //
    // Does not need to be atomic because RunScheduler is the only thread that
    // will ever access this queue.
    deque<Txn*> ready_txns_;
    

    // Queue of completed (but not yet committed/aborted) transactions.
    AtomicQueue<Txn*> completed_txns_;

    // Vector of committed transactions that are used to check any overlap
    // during OCC validation phase.
    AtomicVector<Txn*> committed_txns_;

    // Queue of transaction results (already committed or aborted) to be returned
    // to client.
    AtomicQueue<Txn*> txn_results_;

    // Set of transactions that are currently in the process of parallel
    // validation.
    AtomicSet<Txn*> active_set_;

    // Used it for critical section in parallel occ.
    Mutex active_set_mutex_;

    // Lock Manager used for LOCKING concurrency implementations.
    LockManager* lm_;

    // Used for stopping the continuous loop that runs in the scheduler thread
    bool stopped_;

    // Gives us access to the scheduler thread so that we can wait for it to join later.
    pthread_t scheduler_thread_;

};

#endif  // _TXN_PROCESSOR_H_
