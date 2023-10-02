#include "mvcc_storage.h"
#include <deque>
// Init the storage
void MVCCStorage::InitStorage()
{
    for (int i = 0; i < 1000000; i++)
    {
        Write(i, 0, 0);
        Mutex* key_mutex = new Mutex();
        mutexs_[i]       = key_mutex;
    }
}

// Free memory.
MVCCStorage::~MVCCStorage()
{
    for (auto it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it)
    {
        delete it->second;
    }

    mvcc_data_.clear();

    for (auto it = mutexs_.begin(); it != mutexs_.end(); ++it)
    {
        delete it->second;
    }

    mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list
void MVCCStorage::Lock(Key key)
{
    mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key)
{
    mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value *result, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Iterate the version_lists and return the verion whose write timestamp
    // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return false;
    } else {
        deque<Version*>* dq = mvcc_data_[key];
        Version* v = dq->front();
        if(txn_unique_id > v->max_read_id_) {
            *result = v->value_;
            dq->front()->max_read_id_ = txn_unique_id;
        } else {
            for (deque<Version*>::iterator itr = dq->begin(); itr != dq->end(); itr++){
                Version* version = *itr;
                if (version->max_read_id_ > txn_unique_id){
                    break;
                }  else {
                    *result = version->value_;
                }
            }
        }
        return true;
    }
    
}

// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Before all writes are applied, we need to make sure that each write
    // can be safely applied based on MVCC timestamp ordering protocol. This method
    // only checks one key, so you should call this method for each key in the
    // write_set. Return true if this key passes the check, return false if not.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return true;
    } else {
        Version* v =  mvcc_data_[key]->front();
        if(v->max_read_id_ > txn_unique_id) {
          return false;
        }
        return true;
    }
}


bool MVCCStorage::CheckWrite1(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Before all writes are applied, we need to make sure that each write
    // can be safely applied based on MVCC timestamp ordering protocol. This method
    // only checks one key, so you should call this method for each key in the
    // write_set. Return true if this key passes the check, return false if not.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.

     if (!mvcc_data_.count(key) || (*mvcc_data_[key]).empty()) {
        return true;
    } else {
        Version* v =  mvcc_data_[key]->front();
        if(v->version_id_ > txn_unique_id) {
            return false;
        }
        return true;
    }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
    // into the version_lists. Note that InitStorage() also calls this method to init storage.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.
    // Note that the performance would be much better if you organize the versions in decreasing order.
    Version* version = new Version();
    version->value_ = value;
    version->max_read_id_ = txn_unique_id;
    version->version_id_ = txn_unique_id;
    if (!mvcc_data_[key]) {
        mvcc_data_[key] = new std::deque<Version*>;
    }
    mvcc_data_[key]->push_front(version);
}