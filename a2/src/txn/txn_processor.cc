#include "txn_processor.h"
#include <stdio.h>
#include <set>
#include <unordered_set>
#include <list>  
#include <vector>  
#include <algorithm> 
#include <iterator> 

#include "lock_manager.h"

using namespace std;  
// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode) : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1)
{
    if (mode_ == LOCKING_EXCLUSIVE_ONLY)
        lm_ = new LockManagerA(&ready_txns_);
    else if (mode_ == LOCKING)
        lm_ = new LockManagerB(&ready_txns_);

    // Create the storage
    if (mode_ == MVCC_MVTO || mode_ == MVCC_MV2PL)
    {
        lm_ = new LockManagerB(&ready_txns_);   
        storage_ = new MVCCStorage();
    }
    else
    {
        storage_ = new Storage();
    }

    storage_->InitStorage();

    // Start 'RunScheduler()' running.

    pthread_attr_t attr;
    pthread_attr_init(&attr);

#if !defined(_MSC_VER) && !defined(__APPLE__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < 7; i++)
    {
        CPU_SET(i, &cpuset);
    }
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
#endif

    pthread_t scheduler_;
    pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

    stopped_          = false;
    scheduler_thread_ = scheduler_;
}

void* TxnProcessor::StartScheduler(void* arg)
{
    reinterpret_cast<TxnProcessor*>(arg)->RunScheduler();
    return NULL;
}

TxnProcessor::~TxnProcessor()
{
    // Wait for the scheduler thread to join back before destroying the object and its thread pool.
    stopped_ = true;
    pthread_join(scheduler_thread_, NULL);

    if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING || mode_ == MVCC_MV2PL || mode_ == MVCC_MVTO) delete lm_;

    delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn)
{
    // Atomically assign the txn a new number and add it to the incoming txn
    // requests queue.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult()
{
    Txn* txn;
    while (!txn_results_.Pop(&txn))
    {
        // No result yet. Wait a bit before trying again (to reduce contention on
        // atomic queues).
        usleep(1);
    }
    return txn;
}

void TxnProcessor::RunScheduler()
{
    switch (mode_)
    {
        case SERIAL:
            RunSerialScheduler();
            break;
        case LOCKING:
            RunLockingScheduler();
            break;
        case LOCKING_EXCLUSIVE_ONLY:
            RunLockingScheduler();
            break;
        case OCC_SERIAL_FORWARD_VALIDATION:
            RunOCCSerialSchedulerForwardValidation();
            break;
        case OCC_SERIAL_BACKWARD_VALIDATION:
            RunOCCSerialSchedulerBackwardValidation();
            break;
        case OCC_PARREL_FORWARD_VALIDATION:
            RunOCCParallelSchedulerForwardValidation();
            break;
        case OCC_PARREL_BACKWARD_VALIDATION:
            RunOCCParallelSchedulerBackwardValidation();
            break;
        case MVCC_MVTO:
            RunMVCCMVTOScheduler();
            break;
        case MVCC_MV2PL:
            RunMVCCMV2PLScheduler();
            break;
    }
}

void TxnProcessor::RunSerialScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Get next txn request.
        if (txn_requests_.Pop(&txn))
        {
            // Execute txn.
            ExecuteTxn(txn);

            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }

            // Return result to client.
            txn_results_.Push(txn);
        }
    }
}

void TxnProcessor::RunLockingScheduler()
{
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            bool blocked = false;
            // Request read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                if (!lm_->ReadLock(txn, *it))
                {
                    blocked = true;
                }
            }

            // Request write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                if (!lm_->WriteLock(txn, *it))
                {
                    blocked = true;
                }
            }

            // If all read and write locks were immediately acquired, this txn is
            // ready to be executed.
            if (blocked == false)
            {
                ready_txns_.push_back(txn);
            }
        }

        // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn))
        {
            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }

            // Release read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
            // Release write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }

            // Return result to client.
            txn_results_.Push(txn);
        }

        // Start executing all transactions that have newly acquired all their
        // locks.
        while (ready_txns_.size())
        {
            // Get next ready txn from the queue.
            txn = ready_txns_.front();
            ready_txns_.pop_front();

            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->ExecuteTxn(txn);});
        }
    }
}

void TxnProcessor::ExecuteTxn(Txn* txn)
{
    txn->occ_start_time_ = GetTime();   
    txn->occ_start_idx_ =  committed_txns_.Size();
    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) {
            txn->reads_[*it] = result;
        } 
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();

    // Hand the txn back to the RunScheduler thread.
    completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn)
{
    // Write buffered writes out to storage.
    for (map<Key, Value>::iterator it = txn->writes_.begin(); it != txn->writes_.end(); ++it)
    {
        storage_->Write(it->first, it->second, txn->unique_id_);
    }
}

void TxnProcessor::RunOCCSerialSchedulerForwardValidation() {
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
           tp_.AddTask([this, txn]() { this->ExecuteTxn(txn);});
        }

        // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn)) {
            bool isvalid = SerialValidate(txn);
            if(isvalid) {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
                txn_results_.Push(txn);
            } else {
                txn->reads_.clear();
                txn->writes_.clear();
                txn->status_ = INCOMPLETE;
                mutex_.Lock();
                txn->unique_id_ = next_unique_id_;
                next_unique_id_++;
                txn_requests_.Push(txn);
                mutex_.Unlock();
            }
        }
    }
}


void TxnProcessor::RunOCCSerialSchedulerBackwardValidation() {

    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            tp_.AddTask([this, txn]() { this->ExecuteTxn(txn);});
        }

        // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn)) {
            bool isvalid = true;
            for(int i = txn->occ_start_idx_ + 1; i<committed_txns_.Size() && isvalid;i++) {
                Txn* past_txn = committed_txns_.operator[](i);
               // cout<<past_txn->readset_.size()<<"\n";
                for (set<Key>::iterator it = past_txn->writeset_.begin(); it != past_txn->writeset_.end() && isvalid; ++it){
                    for (set<Key>::iterator it1 = txn->readset_.begin(); it1 != txn->readset_.end(); ++it1)
                    {
                       if(*it1 == *it) {
                         isvalid = false;
                         break;
                       }
                    }
                }
           }
            if(isvalid) {
                ApplyWrites(txn);
                committed_txns_.Push(txn);
                txn->status_ = COMMITTED;
                txn_results_.Push(txn);
            } else {
                txn->reads_.clear();
                txn->writes_.clear();
                txn->status_ = INCOMPLETE;
                mutex_.Lock();
                txn->unique_id_ = next_unique_id_;
                next_unique_id_++;
                txn_requests_.Push(txn);
                mutex_.Unlock();
            } 
        }
       
    }
}

void TxnProcessor::RunOCCParallelSchedulerForwardValidation() {

    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            tp_.AddTask([this, txn]() {this->ExecuteTxnParallelForwardValidation(txn);});
        }
    }

}

void TxnProcessor::RunOCCParallelSchedulerBackwardValidation() {
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            tp_.AddTask([this, txn]() {this->ExecuteTxnParallelBackwardValidation(txn);});
        }
    }
}

void TxnProcessor::RunMVCCMVTOScheduler()
{ 
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            tp_.AddTask([this, txn]() {this->MVCCMVTOExecuteTxn(txn);});
        }
    }
}

void  TxnProcessor::ExecuteTxnParallelBackwardValidation(Txn* txn) {
    // Get the current commited transaction index for the further validation.
    txn->occ_start_idx_ = committed_txns_.Size();
    
    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();
    
    active_set_mutex_.Lock();
    set<Txn*> finish = active_set_.GetSet();
    active_set_.Insert(txn);
    active_set_mutex_.Unlock();
    bool isvalid = true;
    set<Key> interset;
    for (set<Key>::iterator it2 = txn->readset_.begin(); it2 != txn->readset_.end(); ++it2)
    {
        interset.insert(*it2);
    }
    for(int i = txn->occ_start_idx_ + 1; i<committed_txns_.Size() && isvalid;i++) {
        Txn* past_txn = committed_txns_.operator[](i);
        for (map<Key, Value>::iterator it = past_txn->writes_.begin(); it != past_txn->writes_.end() && isvalid; ++it){
            for (set<Key>::iterator it1 = interset.begin(); it1 != interset.end(); ++it1)
            {
                if(*it1 == it->first) {
                    isvalid = false;
                    break;
                }
            }
        }
    }
    if(isvalid) {
        for (set<Key>::iterator it3 = txn->writeset_.begin(); it3 != txn->writeset_.end(); ++it3)
        {
            interset.insert(*it3);
        }
        for (set<Txn*>::iterator it = finish.begin(); it != finish.end() && isvalid; ++it)
        {
            Txn* past_txn = *it;
            for (set<Key>::iterator it1 = past_txn->writeset_.begin(); it1 != past_txn->writeset_.end() && isvalid; ++it1){

                for (set<Key>::iterator it2 = interset.begin(); it2 != interset.end(); ++it2)
                {
                    if(*it1 == *it2) {
                        isvalid = false;
                        break;
                    }
                }
            }
        }
    }
    if(isvalid) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
        committed_txns_.Push(txn);
    } else {
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
    active_set_mutex_.Lock();
    active_set_.Erase(txn);
    active_set_mutex_.Unlock();
    finish.clear();
}


void  TxnProcessor::ExecuteTxnParallelForwardValidation(Txn* txn) {
    // Get the current commited transaction index for the further validation.
    txn->occ_start_time_ = GetTime();
    
    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();
    active_set_mutex_.Lock();
    set<Txn*> finish = active_set_.GetSet();
    active_set_.Insert(txn);
    active_set_mutex_.Unlock();
    bool isvalid = SerialValidate(txn);
    if(isvalid) {
        set<Key> interset;
        for (set<Key>::iterator it2 = txn->readset_.begin(); it2 != txn->readset_.end(); ++it2)
        {
            interset.insert(*it2);
        }
        for (set<Key>::iterator it3 = txn->writeset_.begin(); it3 != txn->writeset_.end(); ++it3)
        {
            interset.insert(*it3);
        }
        for (set<Txn*>::iterator it = finish.begin(); it != finish.end() && isvalid; ++it)
        {
            Txn* past_txn = *it;
            for (set<Key>::iterator it1 = past_txn->writeset_.begin(); it1 != past_txn->writeset_.end() && isvalid; ++it1){

                for (set<Key>::iterator it2 = interset.begin(); it2 != interset.end(); ++it2)
                {
                    if(*it1 == *it2) {
                        isvalid = false;
                        break;
                    }
                }
            }
        }
    }
    if(isvalid) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
        committed_txns_.Push(txn);
    } else {
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
    active_set_mutex_.Lock();
    active_set_.Erase(txn);
    active_set_mutex_.Unlock();
    finish.clear();
}

bool TxnProcessor::SerialValidate(Txn* txn) {
   for (auto&& key : txn->readset_) {
        if (txn->occ_start_time_ < storage_->Timestamp(key)) {
            return false;
        }
           
    }
    for (auto&& key : txn->writeset_) {
        if (txn->occ_start_time_ < storage_->Timestamp(key)) {
            return false;
        }
        
    }
    return true;
}

void TxnProcessor::MVCCMVTOExecuteTxn(Txn* txn) {
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        storage_->Lock(*it);
        if (storage_->Read(*it, &result,txn->unique_id_)) {
            txn->reads_[*it] = result;
        }
        storage_->Unlock(*it);
    }
    txn->Run();
    MVCCLockWriteKeys(txn);
    bool isvalid = MVCCCheckWrites(txn);
    if(isvalid) {
        ApplyWrites(txn);
        MVCCUnlockWriteKeys(txn);
        txn->status_ = COMMITTED;
        txn_results_.Push(txn);
        committed_txns_.Push(txn);
       
    } else {
        MVCCUnlockWriteKeys(txn);
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
}

bool TxnProcessor::MVCCCheckWrites(Txn* txn) {

    bool isvalid = true;
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end() && isvalid; ++it)
    {
        isvalid = storage_->CheckWrite(*it,txn->unique_id_);
    }
    return isvalid;
}

void TxnProcessor::MVCCLockWriteKeys(Txn* txn) {
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        storage_->Lock(*it);
    }
}

void TxnProcessor::MVCCUnlockWriteKeys(Txn* txn) {
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        storage_->Unlock(*it);
    }
}


void TxnProcessor::RunMVCCMV2PLScheduler() {
    Txn* txn;
    while (!stopped_)
    {
        // Start processing the next incoming transaction request.
        if (txn_requests_.Pop(&txn))
        {
            
            bool blocked = false;
            // Request read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                if (!lm_->ReadLock(txn, *it))
                {
                    blocked = true;
                }
            }

            // Request write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                if (!lm_->WriteLock(txn, *it)) {
                    blocked = true;
                }
            }

            // If all read and write locks were immediately acquired, this txn is
            // ready to be executed.
            if (blocked == false)
            {
                ready_txns_.push_back(txn);
            }
        }

         // Process and commit all transactions that have finished running.
        while (completed_txns_.Pop(&txn))
        {
            // Release read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
            // Release write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
            txn_results_.Push(txn);
        }

        while (ready_txns_.size())
        {
            // Get next ready txn from the queue.
            txn = ready_txns_.front();
            ready_txns_.pop_front();
            // Start txn running in its own thread.
            tp_.AddTask([this, txn]() { this->MVCC2PLExecuteTxn(txn);});
        }
    }
}


void TxnProcessor::MVCC2PLExecuteTxn(Txn* txn) {
    txn->Run();
    MVCCLockWriteKeys(txn);
    bool isvalid = true;
    if(isvalid) {
        ApplyWrites(txn);
        MVCCUnlockWriteKeys(txn);
        txn->status_ = COMMITTED;
        committed_txns_.Push(txn);
        completed_txns_.Push(txn);
    } else {
        MVCCUnlockWriteKeys(txn);
        txn->reads_.clear();
        txn->writes_.clear();
        txn->status_ = INCOMPLETE;
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
    }
}


bool TxnProcessor::MVCC2PLCheckWrites(Txn* txn) {
    bool isvalid = true;
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end() && isvalid; ++it)
    {
        isvalid = storage_->CheckWrite1(*it,txn->unique_id_);
    }
    return isvalid;
}