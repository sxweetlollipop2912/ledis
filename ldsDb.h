#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <chrono>

#include "linked_list.h"
#include "ldsKey.h"
#include "ldsVal.h"

struct ldsDb {
    std::unordered_map<std::string, ldsKey> keys;
    llist<ldsVal> vals;
    std::unordered_map<std::string, std::chrono::system_clock::time_point> last_access;
    std::unordered_map<std::string, int> ttl;

    std::shared_timed_mutex vals_mtx{}, keys_mtx{}, last_access_mtx{}, ttl_mtx{};

    ~ldsDb() {
        for (auto it = vals.begin(); it != vals.end(); ++it) {
            switch (it->type)
            {
            case STRING_T:
                delete (std::string *)it->ptr;
                break;
            case LIST_T:
                delete (llist<std::string> *)it->ptr;
                break;
            case SET_T:
                delete (std::set<std::string> *)it->ptr;
                break;
            default:
                break;
            };
        }
    }
};

/* Insert a new key-value pair to db, overwrite last value if necessary */
ldsVal dbInsertKV(ldsDb &db, const std::string &key, int ttl, void *val, unsigned type) {
    ldsKey new_key;
    new_key.key = key;
    new_key.updated_at = std::chrono::system_clock::now();

    std::unique_lock<std::shared_timed_mutex> ulock_key(db.keys_mtx);
    auto it = db.keys.find(key);

    std::unique_lock<std::shared_timed_mutex> ulock_val(db.vals_mtx);
    if (it != db.keys.end()) {
        db.vals.remove(it->second.val_iter);
    }
    new_key.val_iter = db.vals.insert(db.vals.end(), ldsVal{val, type});

    db.keys[key] = new_key;

    std::unique_lock<std::shared_timed_mutex> ulock_ttl(db.ttl_mtx);
    db.ttl[key] = ttl;

    std::unique_lock<std::shared_timed_mutex> ulock_la(db.last_access_mtx);
    db.last_access[key] = std::chrono::system_clock::now();

    return *db.keys[key].val_iter;
}

ldsVal dbModifyVal(ldsDb &db, const std::string &key, std::function<void(ldsVal &)> modifier) {
    std::shared_lock<std::shared_timed_mutex> ulock_key(db.keys_mtx);
    auto it = db.keys.find(key);
    if (it == db.keys.end()) {
        return ldsVal{nullptr, 0};
    }

    std::unique_lock<std::shared_timed_mutex> ulock_val(db.vals_mtx);
    modifier(*it->second.val_iter);

    std::unique_lock<std::shared_timed_mutex> ulock_la(db.last_access_mtx);
    db.last_access[key] = std::chrono::system_clock::now();

    return *it->second.val_iter;
}

/* Get value of a key */
ldsVal dbGetVal(ldsDb &db, const std::string &key) {
    std::shared_lock<std::shared_timed_mutex> slock_key(db.keys_mtx);
    auto it = db.keys.find(key);
    if (it == db.keys.end()) {
        return ldsVal{nullptr, 0};
    }
    auto val = *it->second.val_iter;

    std::unique_lock<std::shared_timed_mutex> ulock_la(db.last_access_mtx);
    db.last_access[key] = std::chrono::system_clock::now();

    return val;
}

/* Remove a key from db */
ldsVal dbDelKey(ldsDb &db, const std::string &key) {
    std::unique_lock<std::shared_timed_mutex> ulock_key(db.keys_mtx);
    auto it = db.keys.find(key);
    if (it == db.keys.end()) {
        return ldsVal{nullptr, 0};
    }
    auto val = *it->second.val_iter;
    db.keys.erase(it);

    std::unique_lock<std::shared_timed_mutex> ulock_val(db.vals_mtx);
    db.vals.remove(it->second.val_iter);

    std::unique_lock<std::shared_timed_mutex> ulock_ttl(db.ttl_mtx);
    db.ttl.erase(key);

    std::unique_lock<std::shared_timed_mutex> ulock_la(db.last_access_mtx);
    db.last_access.erase(key);

    return val;
}

/* Remove all keys from db */
void dbFlush(ldsDb &db) {
    std::unique_lock<std::shared_timed_mutex> ulock_key(db.keys_mtx);
    db.keys.clear();

    std::unique_lock<std::shared_timed_mutex> ulock_val(db.vals_mtx);
    db.vals.clear();

    std::unique_lock<std::shared_timed_mutex> ulock_ttl(db.ttl_mtx);
    db.ttl.clear();

    std::unique_lock<std::shared_timed_mutex> ulock_la(db.last_access_mtx);
    db.last_access.clear();
}
