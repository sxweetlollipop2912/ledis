#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <chrono>

#include "linked_list.h"
#include "ldsKey.h"
#include "ldsVal.h"

class ldsDb {
private:
#define ULOCK(lock, mutex) std::unique_lock<std::shared_timed_mutex> lock(mutex)
#define SLOCK(lock, mutex) std::shared_lock<std::shared_timed_mutex> lock(mutex)
#define UNLOCK(lock) lock.unlock()

    using cval_type = llist<ldsVal>;
    using ckey_type = std::unordered_map<std::string, ldsKey>;
    using cla_type = std::unordered_map<std::string, std::chrono::system_clock::time_point>;

    ckey_type keys;
    cval_type vals;
    cla_type last_access;

    std::shared_timed_mutex vals_mtx{}, keys_mtx{}, last_access_mtx{};

    /* Check if key has expired
     * Precondition:
     * - acquire shared lock on keys_mtx
     * */
    bool isExpired(ckey_type::iterator key_iter) {
        if (key_iter == keys.end()) {
            return false;
        }
        if (!key_iter->second.ttl.has_value()) {
            return false;
        }
        return std::chrono::system_clock::now() >= key_iter->second.ttl.value();
    }

    /* Get value of a key
     * Precondition:
     * - acquire shared lock on keys_mtx
     * - acquire shared lock on vals_mtx
     * */
    cval_type::iterator getValIter(ckey_type::iterator key_iter) {
        if (key_iter == keys.end() || key_iter->second.zombie) {
            return vals.end();
        }
        return key_iter->second.val_iter;
    }

    /* Delete a value from db.vals
     * Precondition:
     * - acquire unique lock on vals_mtx
     * */
    bool deleteVal(cval_type::iterator val_iter) {
        if (val_iter == vals.end()) {
            return false;
        }

        switch (val_iter->type) {
            case STRING_T:
                delete (std::string *) val_iter->ptr;
                break;
            case LIST_T:
                delete (llist<std::string> *) val_iter->ptr;
                break;
            case SET_T:
                delete (std::set<std::string> *) val_iter->ptr;
                break;
            default:
                throw std::runtime_error("Invalid value type id: " + std::to_string(val_iter->type));
        }

        vals.remove(val_iter);
        return true;
    }

    /* Write a new key-value pair to db, overwrite last value if necessary
     * Precondition:
     * - acquire unique lock on keys_mtx
     * - acquire unique lock on vals_mtx
     * - acquire unique lock on last_access_mtx
     * */
    std::tuple<ckey_type::iterator, cval_type::iterator> writeKV(const std::string &key, void *val, unsigned type) {
        ldsKey new_key;
        new_key.key = key;

        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        deleteVal(it);
        new_key.val_iter = vals.insert(vals.end(), ldsVal{val, type});

        keys[key] = new_key;
        last_access[key] = std::chrono::system_clock::now();

        return {keys.find(key), new_key.val_iter};
    }

    /* Modify value of a key
     * Precondition:
     * - acquire shared lock on keys_mtx
     * - acquire unique lock on vals_mtx
     * */
    cval_type::iterator modifyVal(ckey_type::iterator key_iter, const std::function<void(ldsVal &)> &modifier) {
        if (key_iter == keys.end()) {
            return vals.end();
        }
        modifier(*key_iter->second.val_iter);
        return key_iter->second.val_iter;
    }

    /* Remove a key from db.keys, db.ttl, db.last_access
     * Precondition:
     * - acquire unique lock on keys_mtx
     * - acquire unique lock on last_access_mtx
     * */
    bool deleteKey(const std::string &key) {
        auto it = keys.find(key);
        if (it == keys.end()) {
            return false;
        }
        keys.erase(it);
        last_access.erase(key);
        return true;
    }

    /* Delete key if it has expired
     * Precondition:
     * - acquire unique lock on keys_mtx
     * - acquire unique lock on vals_mtx
     * - acquire unique lock on ttl_mtx
     * - acquire unique lock on last_access_mtx
     * */
    bool deleteKV(const std::string &key) {
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return false;
        }
        auto val_iter = key_iter->second.val_iter;
        deleteVal(val_iter);
        return deleteKey(key);
    }

public:
    ldsDb() = default;

    ~ldsDb() {
        while (!vals.empty()) {
            deleteVal(vals.begin());
        }
    }

    /* PRE/POST-COMMAND OPERATIONS */

    void preCommand(const std::vector<std::string> &keys, bool all_keys = false) {
        SLOCK(slock_key, keys_mtx);
        std::vector<ckey_type::iterator> to_delete;
        auto process = [this, &to_delete](ckey_type::iterator key_iter) {
            if (key_iter == this->keys.end()) {
                return;
            }
            if (isExpired(key_iter)) {
                to_delete.push_back(key_iter);
            }
        };
        if (all_keys) {
            for (auto it = this->keys.begin(); it != this->keys.end(); it++) {
                process(it);
            }
        } else {
            for (auto &key: keys) {
                process(this->keys.find(key));
            }
        }
        if (!to_delete.empty()) {
            UNLOCK(slock_key);
            ULOCK(ulock_key, keys_mtx);
            ULOCK(ulock_val, vals_mtx);
            ULOCK(ulock_la, last_access_mtx);
            for (auto &key_iter: to_delete) {
                deleteKV(key_iter->first);
            }
        }
    }

    void postAccessCommand(const std::vector<std::string> &keys, bool all_keys = false) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_la, last_access_mtx);
        auto now = std::chrono::system_clock::now();
        auto process = [this, &now](ckey_type::iterator key_iter) {
            if (key_iter == this->keys.end()) {
                return;
            }
            this->last_access[key_iter->first] = now;
        };
        if (all_keys) {
            for (auto it = this->keys.begin(); it != this->keys.end(); it++) {
                process(it);
            }
        } else {
            for (auto &key: keys) {
                process(this->keys.find(key));
            }
        }
    }

    /* GENERIC OPERATIONS */

    /* Get list of keys */
    std::vector<std::string> getKeys() {
        SLOCK(slock_key, keys_mtx);
        std::vector<std::string> ret;
        for (auto &[key, _]: keys) {
            ret.push_back(key);
        }
        return ret;
    }

    /* Delete a key from db */
    bool del(const std::string &key) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        ULOCK(ulock_la, last_access_mtx);
        return deleteKV(key);
    }

    /* Remove all keys from db */
    void flush() {
        ULOCK(ulock_key, keys_mtx);
        keys.clear();

        ULOCK(ulock_val, vals_mtx);
        while (!vals.empty()) {
            deleteVal(vals.begin());
        }

        ULOCK(ulock_la, last_access_mtx);
        last_access.clear();
    }

    /* TTL OPERATIONS */

    /* Get time-to-live of a key */
    std::optional<int> getTTL(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return std::nullopt;
        }
        if (key_iter->second.zombie) {
            UNLOCK(slock_key);
            ULOCK(ulock_key, keys_mtx);
            ULOCK(ulock_val, vals_mtx);
            ULOCK(ulock_la, last_access_mtx);
            deleteKV(key);
            return std::nullopt;
        }

        if (!key_iter->second.ttl.has_value()) {
            return std::nullopt;
        }
        auto ret = key_iter->second.ttl.value() - std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(ret).count();
    }

    /* Set time-to-live of a key */
    std::optional<int> setTTL(const std::string &key, int ttl) {
        ULOCK(ulock_key, keys_mtx);
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return std::nullopt;
        }
        if (key_iter->second.zombie) {
            ULOCK(ulock_val, vals_mtx);
            ULOCK(ulock_la, last_access_mtx);
            deleteKV(key);
            return std::nullopt;
        }

        if (ttl < 0) {
            key_iter->second.ttl = std::chrono::system_clock::now();
        } else
            key_iter->second.ttl = std::chrono::system_clock::now() + std::chrono::seconds(ttl);
        return std::chrono::duration_cast<std::chrono::seconds>(
                key_iter->second.ttl.value() - std::chrono::system_clock::now()).count();
    }

    /* STRING OPERATIONS */

    std::optional<std::string> getStr(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto it = getValIter(keys.find(key));
        if (it == vals.end()) {
            return std::nullopt;
        }
        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return *ldsValToStr(*it);
    }

    void setStr(const std::string &key, const std::string &val) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        ULOCK(ulock_la, last_access_mtx);
        writeKV(key, new std::string{val}, STRING_T);
    }

    /* LIST OPERATIONS */

    typedef unsigned int llen_t;
#define LFRONT 0
#define LBACK 1

    /* Get length of list */
    llen_t getListLen(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return 0;
        }

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();
        return ldsValToList(*it)->size();
    }

    /* Push a value to the front/back of a list */
    llen_t pushList(const std::string &key, std::string val, unsigned where) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            // key does not exist, create a new list
            ULOCK(ulock_la, last_access_mtx);
            auto [_, val_iter] = writeKV(key, new llist<std::string>{val}, LIST_T);
            return ldsValToList(*val_iter)->size();
        }

        auto ldsVal = *modifyVal(key_iter, [&val, where](struct ldsVal &v) {
            auto list = ldsValToList(v);
            if (where == LFRONT) {
                list->push_front(val);
            } else if (where == LBACK) {
                list->push_back(val);
            } else {
                throw std::runtime_error("Invalid push location id: " + std::to_string(where));
            }
        });

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ldsValToList(ldsVal)->size();
    }

    std::optional<std::string> popList(const std::string &key, unsigned where) {
        SLOCK(slock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return std::nullopt;
        }

        std::string ret;
        auto ldsVal = *modifyVal(key_iter, [where, &ret](struct ldsVal &v) {
            auto list = ldsValToList(v);
            if (list->empty()) {
                return;
            }
            if (where == LFRONT) {
                ret = list->front();
                list->pop_front();
            } else if (where == LBACK) {
                ret = list->back();
                list->pop_back();
            } else {
                throw std::runtime_error("Invalid pop location id: " + std::to_string(where));
            }
        });

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ret;
    }

    std::vector<std::string> rangeList(const std::string &key, int start, int stop) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return {};
        }

        auto list = ldsValToList(*it);
        if (start < 0) {
            start += list->size();
        }
        if (start < 0) {
            start = 0;
        }
        if (stop < 0) {
            stop += list->size();
        }
        if (stop >= list->size()) {
            stop = list->size() - 1;
        }
        if (start >= list->size()) {
            return {};
        }
        if (start > stop) {
            return {};
        }

        std::vector<std::string> ret;
        auto it2 = list->begin() + start;
        for (int i = start; i <= stop && it2 != list->end(); i++, it2++) {
            ret.push_back(*it2);
        }

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ret;
    }

    /* SET OPERATIONS */

    typedef size_t slen_t;

    slen_t getSetLen(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return 0;
        }

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ldsValToSet(*it)->size();
    }

    std::vector<std::string> getSetMems(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return {};
        }

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        auto set = ldsValToSet(*it);
        return {set->begin(), set->end()};
    }

    std::vector<std::string> getSetInter(const std::vector<std::string> &keys) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        std::vector<std::set<std::string> *> sets;
        for (auto &key: keys) {
            auto key_iter = this->keys.find(key);
            auto it = getValIter(key_iter);
            if (it == vals.end()) {
                return {};
            }
            sets.push_back(ldsValToSet(*it));
        }

        if (sets.empty()) {
            return {};
        }

        std::vector<std::string> ret(sets[0]->begin(), sets[0]->end());
        for (int i = 1; i < sets.size(); i++) {
            std::vector<std::string> tmp;
            std::set_intersection(ret.begin(), ret.end(), sets[i]->begin(), sets[i]->end(), std::back_inserter(tmp));
            ret = std::move(tmp);
        }

        ULOCK(ulock_la, last_access_mtx);
        for (auto &key: keys) {
            last_access[key] = std::chrono::system_clock::now();
        }

        return {ret.begin(), ret.end()};
    }

    slen_t insertSet(const std::string &key, const std::vector<std::string> &vals) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == this->vals.end()) {
            // key does not exist, create a new set
            ULOCK(ulock_la, last_access_mtx);
            auto [_, val_iter] = writeKV(key, new std::set<std::string>{vals.begin(), vals.end()}, SET_T);
            return ldsValToSet(*val_iter)->size();
        }

        auto ldsVal = *modifyVal(key_iter, [&vals](struct ldsVal &v) {
            auto set = ldsValToSet(v);
            set->insert(vals.begin(), vals.end());
        });

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ldsValToSet(ldsVal)->size();
    }

    slen_t removeSet(const std::string &key, const std::vector<std::string> &vals) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == this->vals.end()) {
            return 0;
        }

        slen_t ret = 0;
        auto ldsVal = *modifyVal(key_iter, [&vals, &ret](struct ldsVal &v) {
            auto set = ldsValToSet(v);
            for (auto &val: vals) {
                ret += set->erase(val);
            }
        });

        ULOCK(ulock_la, last_access_mtx);
        last_access[key] = std::chrono::system_clock::now();

        return ret;
    }
};
