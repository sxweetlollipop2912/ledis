#pragma once

#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <optional>
#include <list>

#include "ldsKey.h"
#include "ldsVal.h"
#include "ldsCmd.h"
#include "logger.h"

extern logger LOGGER;

class ldsDb {
public:
    typedef unsigned int llen_t;
    typedef size_t slen_t;
#define LFRONT 0
#define LBACK 1

private:
#define ULOCK(lock, mutex) std::unique_lock<std::shared_timed_mutex> lock(mutex)
#define SLOCK(lock, mutex) std::shared_lock<std::shared_timed_mutex> lock(mutex)
#define UNLOCK(lock) lock.unlock()

    using cval_type = std::list<ldsVal>;
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
        if (key_iter == keys.end()) {
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
                delete (std::list<std::string> *) val_iter->ptr;
                break;
            case SET_T:
                delete (std::set<std::string> *) val_iter->ptr;
                break;
            default:
                throw std::runtime_error("Invalid value type id: " + std::to_string(val_iter->type));
        }

        vals.erase(val_iter);
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

    /* Delete key if it has expired
     * Precondition:
     * - acquire unique lock on keys_mtx
     * - acquire unique lock on vals_mtx

     * - acquire unique lock on last_access_mtx
     * */
    bool deleteKV(const std::string &key) {
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return false;
        }
        // Delete value
        auto val_iter = key_iter->second.val_iter;
        deleteVal(val_iter);

        // Delete key
        keys.erase(key_iter);
        last_access.erase(key);
        return true;
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
    int getTTL(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return -2;
        }
        if (!key_iter->second.ttl.has_value()) {
            return -1;
        }
        auto ret = key_iter->second.ttl.value() - std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(ret).count();
    }

    /* Set time-to-live of a key */
    int setTTL(const std::string &key, int ttl) {
        if (ttl < 0) {
            throw std::runtime_error("Invalid TTL value: " + std::to_string(ttl) + " (must be >= 0)");
        }
        ULOCK(ulock_key, keys_mtx);
        auto key_iter = keys.find(key);
        if (key_iter == keys.end()) {
            return -2;
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

        return *ldsValToStr(*it);
    }

    void setStr(const std::string &key, const std::string &val) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        ULOCK(ulock_la, last_access_mtx);
        writeKV(key, new std::string{val}, STRING_T);
    }

    /* LIST OPERATIONS */

    /* Get length of list */
    llen_t getListLen(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return 0;
        }
        return ldsValToList(*it)->size();
    }

    /* Push a value to the front/back of a list */
    llen_t pushList(const std::string &key, const std::vector<std::string> &vals, unsigned where) {
        ULOCK(ulock_key, keys_mtx);
        ULOCK(ulock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == this->vals.end()) {
            // key does not exist, create a new list
            LOGGER.debug("Key does not exist, creating new list");
            ULOCK(ulock_la, last_access_mtx);
            auto [_, val_iter] = writeKV(key, where == LBACK ? new std::list<std::string>{vals.begin(), vals.end()}
                                                             : new std::list<std::string>{vals.rbegin(), vals.rend()},
                                         LIST_T);
            return ldsValToList(*val_iter)->size();
        }

        auto ldsVal = *modifyVal(key_iter, [&vals, where](struct ldsVal &v) {
            auto list = ldsValToList(v);
            if (where == LFRONT) {
                list->insert(list->begin(), vals.rbegin(), vals.rend());
            } else if (where == LBACK) {
                list->insert(list->end(), vals.begin(), vals.end());
            } else {
                throw std::runtime_error("Invalid push location id: " + std::to_string(where));
            }
        });

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
        if (ldsValToList(ldsVal)->empty()) {
            ULOCK(ulock_la, last_access_mtx);
            deleteKV(key);
        }

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
        auto it2 = std::next(list->begin(), start);
        for (int i = start; i <= stop && it2 != list->end(); i++, it2++) {
            ret.push_back(*it2);
        }

        return ret;
    }

    /* SET OPERATIONS */

    slen_t getSetLen(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        SLOCK(slock_val, vals_mtx);
        auto key_iter = keys.find(key);
        auto it = getValIter(key_iter);
        if (it == vals.end()) {
            return 0;
        }

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

        slen_t ret = 0;
        auto ldsVal = *modifyVal(key_iter, [&vals, &ret](struct ldsVal &v) {
            auto set = ldsValToSet(v);
            ret = set->size();
            set->insert(vals.begin(), vals.end());
            ret = set->size() - ret;
        });

        return ret;
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
        if (ldsValToSet(ldsVal)->empty()) {
            ULOCK(ulock_la, last_access_mtx);
            deleteKV(key);
        }

        return ret;
    }

public:
    ldsDb() = default;

    ~ldsDb() {
        while (!vals.empty()) {
            deleteVal(vals.begin());
        }
    }

    /* GENERIC OPERATIONS */
    std::vector<std::string> cmdKeys() {
        preCommand({}, true);
        return getKeys();
    }

    bool cmdDel(const std::string &key) {
        preCommand({key});
        return del(key);
    }

    void cmdFlush() {
        flush();
    }

    int cmdTTL(const std::string &key) {
        preCommand({key});
        return getTTL(key);
    }

    int cmdExpire(const std::string &key, int ttl) {
        preCommand({key});
        return setTTL(key, ttl);
    }

    /* STRING OPERATIONS */
    std::optional<std::string> cmdGet(const std::string &key) {
        preCommand({key});
        auto ret = getStr(key);
        postAccessCommand({key});
        return ret;
    }

    void cmdSet(const std::string &key, const std::string &val) {
        preCommand({key});
        setStr(key, val);
        postAccessCommand({key});
    }

    /* LIST OPERATIONS */
    llen_t cmdLlen(const std::string &key) {
        preCommand({key});
        auto ret = getListLen(key);
        postAccessCommand({key});
        return ret;
    }

    llen_t cmdPush(const std::string &key, const std::vector<std::string> &vals, unsigned where) {
        preCommand({key});
        auto ret = pushList(key, vals, where);
        postAccessCommand({key});
        return ret;
    }

    std::optional<std::string> cmdPop(const std::string &key, unsigned where) {
        preCommand({key});
        auto ret = popList(key, where);
        postAccessCommand({key});
        return ret;
    }

    std::vector<std::string> cmdLrange(const std::string &key, int start, int stop) {
        preCommand({key});
        auto ret = rangeList(key, start, stop);
        postAccessCommand({key});
        return ret;
    }

    /* SET OPERATIONS */
    slen_t cmdScard(const std::string &key) {
        preCommand({key});
        auto ret = getSetLen(key);
        postAccessCommand({key});
        return ret;
    }

    std::vector<std::string> cmdSmembers(const std::string &key) {
        preCommand({key});
        auto ret = getSetMems(key);
        postAccessCommand({key});
        return ret;
    }

    std::vector<std::string> cmdSinter(const std::vector<std::string> &keys) {
        preCommand(keys);
        auto ret = getSetInter(keys);
        postAccessCommand(keys);
        return ret;
    }

    slen_t cmdSadd(const std::string &key, const std::vector<std::string> &vals) {
        preCommand({key});
        auto ret = insertSet(key, vals);
        postAccessCommand({key});
        return ret;
    }

    slen_t cmdSrem(const std::string &key, const std::vector<std::string> &vals) {
        preCommand({key});
        auto ret = removeSet(key, vals);
        postAccessCommand({key});
        return ret;
    }

    void execute(const ldsCmd &cmd, ldsRet &ret) {
        auto args = parseArgs(cmd.args);
        switch (cmd.cmd) {
            case CMD_GKEYS:
                LOGGER.info(std::string("[COMMAND] Keys, args: ") + cmd.args);
                if (args.size() != 0) {
                    throw std::runtime_error("Invalid number of arguments for KEYS command");
                }
                ret.ptr = new std::vector<std::string>(cmdKeys());
                ret.type = RET_LIST;
                break;
            case CMD_GDEL:
                LOGGER.info(std::string("[COMMAND] Del, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for DEL command");
                }
                ret.ptr = new bool(cmdDel(args[0]));
                ret.type = RET_BOOL;
                break;
            case CMD_GFLUSHDB:
                LOGGER.info(std::string("[COMMAND] FlushDB, args: ") + cmd.args);
                if (args.size() != 0) {
                    throw std::runtime_error("Invalid number of arguments for FLUSHDB command");
                }
                cmdFlush();
                ret.ptr = nullptr;
                ret.type = RET_OK;
                break;
            case CMD_GTTL:
                LOGGER.info(std::string("[COMMAND] Ttl, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for TTL command");
                }
                ret.ptr = new int(cmdTTL(args[0]));
                ret.type = RET_INT;
                break;
            case CMD_GEXPIRE:
                LOGGER.info(std::string("[COMMAND] Expire, args: ") + cmd.args);
                if (args.size() != 2) {
                    throw std::runtime_error("Invalid number of arguments for EXPIRE command");
                }
                ret.ptr = new int(cmdExpire(args[0], std::stoi(args[1])));
                ret.type = RET_INT;
                break;
            case CMD_RPUSH:
            case CMD_LPUSH:
                LOGGER.info(std::string("[COMMAND] Push, args: ") + cmd.args);
                if (args.size() < 2) {
                    throw std::runtime_error("Invalid number of arguments for RPUSH/LPUSH command");
                }
                ret.ptr = new llen_t(
                        cmdPush(args[0], {args.begin() + 1, args.end()}, cmd.cmd == CMD_RPUSH ? LBACK : LFRONT));
                ret.type = RET_INT;
                break;
            case CMD_LLEN:
                LOGGER.info(std::string("[COMMAND] Llen, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for LLEN command");
                }
                ret.ptr = new llen_t(cmdLlen(args[0]));
                ret.type = RET_INT;
                break;
            case CMD_LPOP:
            case CMD_RPOP: {
                LOGGER.info(std::string("[COMMAND] Pop, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for LPOP/RPOP command");
                }

                auto tmp = cmdPop(args[0], cmd.cmd == CMD_LPOP ? LFRONT : LBACK);
                ret.ptr = tmp ? new std::string(*tmp) : nullptr;
                ret.type = RET_STR;
                break;
            }
            case CMD_LRANGE:
                LOGGER.info(std::string("[COMMAND] Lrange, args: ") + cmd.args);
                if (args.size() != 3) {
                    throw std::runtime_error("Invalid number of arguments for LRANGE command");
                }
                ret.ptr = new std::vector<std::string>(cmdLrange(args[0], std::stoi(args[1]), std::stoi(args[2])));
                ret.type = RET_LIST;
                break;
            case CMD_SADD:
                LOGGER.info(std::string("[COMMAND] Sadd, args: ") + cmd.args);
                if (args.size() < 2) {
                    throw std::runtime_error("Invalid number of arguments for SADD command");
                }
                ret.ptr = new slen_t(cmdSadd(args[0], {args.begin() + 1, args.end()}));
                ret.type = RET_INT;
                break;
            case CMD_SREM:
                LOGGER.info(std::string("[COMMAND] Srem, args: ") + cmd.args);
                if (args.size() < 2) {
                    throw std::runtime_error("Invalid number of arguments for SREM command");
                }
                ret.ptr = new slen_t(cmdSrem(args[0], {args.begin() + 1, args.end()}));
                ret.type = RET_INT;
                break;
            case CMD_SCARD:
                LOGGER.info(std::string("[COMMAND] Scard, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for SCARD command");
                }
                ret.ptr = new slen_t(cmdScard(args[0]));
                ret.type = RET_INT;
                break;
            case CMD_SMEMBERS:
                LOGGER.info(std::string("[COMMAND] Smembers, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for SMEMBERS command");
                }
                ret.ptr = new std::vector<std::string>(cmdSmembers(args[0]));
                ret.type = RET_LIST;
                break;
            case CMD_SINTER:
                LOGGER.info(std::string("[COMMAND] Sinter, args: ") + cmd.args);
                if (args.size() < 2) {
                    throw std::runtime_error("Invalid number of arguments for SINTER command");
                }
                ret.ptr = new std::vector<std::string>(cmdSinter({args.begin(), args.end()}));
                ret.type = RET_LIST;
                break;
            case CMD_SGET: {
                LOGGER.info(std::string("[COMMAND] Get, args: ") + cmd.args);
                if (args.size() != 1) {
                    throw std::runtime_error("Invalid number of arguments for GET command");
                }
                auto tmp = cmdGet(args[0]);
                ret.ptr = tmp ? new std::string(*tmp) : nullptr;
                ret.type = RET_STR;
                break;
            }
            case CMD_SSET:
                LOGGER.info(std::string("[COMMAND] Set, args: ") + cmd.args);
                if (args.size() != 2) {
                    throw std::runtime_error("Invalid number of arguments for SET command");
                }
                cmdSet(args[0], args[1]);
                ret.ptr = nullptr;
                ret.type = RET_OK;
                break;
            default:
                ret.ptr = nullptr;
                ret.type = RET_UNKNOWN;
        }
    }

    /* Check if a key exists in db */
    bool findKey(const std::string &key) {
        SLOCK(slock_key, keys_mtx);
        return keys.find(key) != keys.end();
    }
};
