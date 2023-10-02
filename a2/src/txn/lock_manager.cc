#include "lock_manager.h"
#include <set>
using std::deque;


LockManagerA::LockManagerA(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }


bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
    
   // Implement this method!
    LockRequest l(LockMode::EXCLUSIVE,txn);
    if(lock_table_[key]) {
        lock_table_[key]->push_back(l);
    } else {
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
    }

    if(lock_table_[key]->size() == 1) {
        return true;
    } else {
        txn_waits_[txn]++; 
        return false;
    }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key)
{
    // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
    // simply use the same logic as 'WriteLock'.
    return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
    //
    // Implement this method!
    
    if(lock_table_[key]) {
        Txn* tx1 = lock_table_[key]->front().txn_;
        if(tx1 == txn) {
            lock_table_[key]->pop_front();
            txn_waits_.erase(txn);
            if(lock_table_[key]->size() > 0) {
                Txn* txn1 = lock_table_[key]->front().txn_;
                txn_waits_[txn1] = txn_waits_[txn1] - 1;
                if(txn_waits_[txn1] == 0) {
                    ready_txns_->push_back(txn1);
                }
            }
        } else {
           deque<LockRequest>* queue = lock_table_[key];
           deque<LockRequest>::iterator it1;
           for(it1=queue->begin(); it1 != queue->end();++it1) {
              if(it1->txn_ == txn) {
                lock_table_[key]->erase(it1);
                break;
              }
            }
            txn_waits_.erase(txn);
        }
    }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners)
{
    //
    // Implement this method!
    if(lock_table_.find(key) == lock_table_.end()) {
        return LockMode::UNLOCKED;
    } else {
        owners->clear();
        deque<LockRequest>* queue = lock_table_[key];
        owners->push_back(queue->front().txn_);
        return LockMode::EXCLUSIVE;
    }
}



LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
     ready_txns_ = ready_txns;
      }

bool LockManagerB::WriteLock(Txn* txn, const Key& key)
{
    //
    // // Implement this method!
    LockRequest l(LockMode::EXCLUSIVE,txn);
    if(lock_table_[key]) {
        lock_table_[key]->push_back(l);
    } else {
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
    }
    if(lock_table_[key]->size() == 1) {
        return true;
    } else {
        txn_waits_[txn]++; 
        return false;
    }
     
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {

    
    // //Implement this method!
    LockRequest l(LockMode::SHARED,txn);
    if(lock_table_[key]) {
        lock_table_[key]->push_back(l);
    } else {
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
    }
    if(lock_table_[key]->size() == 1) {
        return true;
    } else {
        deque<LockRequest>* queue = lock_table_[key];
        deque<LockRequest>::iterator it1;
        bool isContainsExclusiveLock = false;
        for(it1=queue->begin()+1; it1 != queue->end();++it1) {
            if(it1->mode_ == LockMode::EXCLUSIVE) {
               isContainsExclusiveLock = true;
               break;
            }
        }
        if(isContainsExclusiveLock) {
            txn_waits_[txn]++; 
            return false;
        } else {
            return true;
        }
    }
}

void LockManagerB::Release(Txn* txn, const Key& key)
{
    
    //Implement this method!
    if(lock_table_[key]) {
        LockMode lock_mode;
        deque<LockRequest>* queue = lock_table_[key];
        deque<LockRequest>::iterator it1,it2;
        bool isA = false;
        bool isB = false;
        int ct = 0;
        for(it1=queue->begin(); it1 != queue->end();it1++) {
            ct++;
            if(it1->txn_ == txn) {
                lock_mode = it1->mode_;
                lock_table_[key]->erase(it1);
                break;
            }
            if(it1->mode_ == LockMode::EXCLUSIVE && !isA) {
                isA = true;
            }
            if(it1->mode_ == LockMode::SHARED && !isB) {
                isB = true;
            }
        }
        if(lock_mode == LockMode::SHARED && !isB && !isA) {
            if(queue->size() > 0) {
                if(queue->front().mode_ == LockMode::EXCLUSIVE) {
                    Txn * t = queue->front().txn_;
                    txn_waits_[t]--;
                    if(txn_waits_[t] == 0) {
                        ready_txns_->push_back(t);
                        txn_waits_.erase(t);
                    }
                }
            }
        }
        if(lock_mode == LockMode::EXCLUSIVE && !isA) {
           int ct1 = 0;
           for(it2=queue->begin()+ct-1; it2 != queue->end();it2++) {
                if(it2->mode_ == LockMode::SHARED) {
                    ct1++;
                    txn_waits_[it2->txn_]--;
                    if(txn_waits_[it2->txn_] == 0) {
                        ready_txns_->push_back(it2->txn_);
                        txn_waits_.erase(it2->txn_);
                    }
                } else {
                    if(ct1 == 0) {
                        txn_waits_[it2->txn_]--;
                        if(txn_waits_[it2->txn_] == 0) {
                            ready_txns_->push_back(it2->txn_);
                            txn_waits_.erase(it2->txn_);
                        }
                    }
                    break;
                }
            }
        }
    }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners)
{
    //
    // Implement this method!
     if(lock_table_[key]) {
        if(lock_table_[key]->size() == 0) {
            return LockMode::UNLOCKED;
        } else {
            owners->clear();
            deque<LockRequest>* queue = lock_table_[key];
            if(queue->front().mode_ == LockMode::EXCLUSIVE) {
                owners->push_back(queue->front().txn_);
                return LockMode::EXCLUSIVE;
            } else {
                deque<LockRequest>::iterator it1;
                for(it1=queue->begin(); it1 != queue->end();++it1) {
                    if(it1->mode_ == LockMode::SHARED) {
                        owners->push_back(it1->txn_);
                    } else {
                        break;
                    }
                }
                return LockMode::SHARED;
            }    
        }
    }
}

