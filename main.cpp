#include <chrono>
#include <iostream>  // cout
#include <thread>  // this_thread
#include <vector>

#include "ldsDb.h"

int main() {
    auto ledisDb = new ldsDb{};

    for(std::string cmd = ""; std::cin >> cmd;) {
        // turn cmd to lowercase
        for (auto &c : cmd) {
            c = std::tolower(c);
        }

        if (cmd == "exit") {
            break;
        }
        try {
            if (cmd == "set") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                dbInsertKV(*ledisDb, key, -1, new std::string(val), STRING_T);
                std::cout << "OK\n";
            }
            else if (cmd == "get") {
                std::string key;
                std::cin >> key;
                auto val = dbGetVal(*ledisDb, key);
                if (val.ptr == nullptr) {
                    std::cout << "(nil)\n";
                } else {
                    std::cout << "\"" << *ldsValToStr(val) << "\"\n";
                }
            }
            else if (cmd == "llen") {
                std::string key;
                std::cin >> key;
                auto val = dbGetVal(*ledisDb, key);
                if (val.ptr == nullptr) {
                    std::cout << "0\n";
                } else {
                    auto list = ldsValToList(val);
                    std::cout << list->size() << '\n';
                }
            }
            else if (cmd == "lpush") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                auto l = dbGetVal(*ledisDb, key);
                if (l.ptr == nullptr) {
                    auto inserted = dbInsertKV(*ledisDb, key, -1, new llist<std::string>{val}, LIST_T);
                    std::cout << ldsValToList(inserted)->size() << '\n';
                } else {
                    dbModifyVal(*ledisDb, key, [&val](ldsVal &v) {
                        auto list = ldsValToList(v);
                        list->push_front(val);
                    });
                    auto list = ldsValToList(dbGetVal(*ledisDb, key));
                    std::cout << list->size() << '\n';
                }
            }
            else if (cmd == "rpush") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                auto l = dbGetVal(*ledisDb, key);
                if (l.ptr == nullptr) {
                    auto inserted = dbInsertKV(*ledisDb, key, -1, new llist<std::string>{val}, LIST_T);
                    std::cout << ldsValToList(inserted)->size() << '\n';
                } else {
                    dbModifyVal(*ledisDb, key, [&val](ldsVal &v) {
                        auto list = ldsValToList(v);
                        list->push_back(val);
                    });
                    auto list = ldsValToList(dbGetVal(*ledisDb, key));
                    std::cout << list->size() << '\n';
                }
            }
            else if (cmd == "lpop") {
                std::string key;
                std::cin >> key;
                auto l = dbGetVal(*ledisDb, key);
                if (l.ptr == nullptr) {
                    std::cout << "(nil)\n";
                } else {
                    auto list = ldsValToList(l);
                    if (list->empty()) {
                        std::cout << "(nil)\n";
                    } else {
                        std::cout << "\"" << list->front() << "\"\n";
                        dbModifyVal(*ledisDb, key, [](ldsVal &v) {
                            auto list = ldsValToList(v);
                            list->pop_front();
                        });
                    }
                }
            }
            else if (cmd == "rpop") {
                std::string key;
                std::cin >> key;
                auto l = dbGetVal(*ledisDb, key);
                if (l.ptr == nullptr) {
                    std::cout << "(nil)\n";
                } else {
                    auto list = ldsValToList(l);
                    if (list->empty()) {
                        std::cout << "(nil)\n";
                    } else {
                        std::cout << "\"" << list->back() << "\"\n";
                        dbModifyVal(*ledisDb, key, [](ldsVal &v) {
                            auto list = ldsValToList(v);
                            list->pop_back();
                        });
                    }
                }
            }
            else if (cmd == "lrange") {
                std::string key;
                std::cin >> key;
                int start, stop;
                std::cin >> start >> stop;
                auto l = dbGetVal(*ledisDb, key);
                if (l.ptr == nullptr) {
                    std::cout << "(nil)\n";
                } else {
                    auto list = ldsValToList(l);
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
                    auto it = list->begin() + start;
                    for (int i = start; i <= stop && it != list->end(); i++, it++) {
                        std::cout << (i - start + 1) << ") \"" << *it << "\"\n";
                    }
                }
            }
            else if (cmd == "sadd") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                auto s = dbGetVal(*ledisDb, key);
                if (s.ptr == nullptr) {
                    auto inserted = dbInsertKV(*ledisDb, key, -1, new std::set<std::string>{val}, SET_T);
                    std::cout << ldsValToSet(inserted)->size() << '\n';
                } else {
                    dbModifyVal(*ledisDb, key, [&val](ldsVal &v) {
                        auto set = ldsValToSet(v);
                        set->insert(val);
                    });
                    auto set = ldsValToSet(dbGetVal(*ledisDb, key));
                    std::cout << set->size() << '\n';
                }
            }
            else if (cmd == "scard") {
                std::string key;
                std::cin >> key;
                auto s = dbGetVal(*ledisDb, key);
                if (s.ptr == nullptr) {
                    std::cout << "0\n";
                } else {
                    auto set = ldsValToSet(s);
                    std::cout << set->size() << '\n';
                }
            }
            else if (cmd == "smembers") {
                std::string key;
                std::cin >> key;
                auto s = dbGetVal(*ledisDb, key);
                if (s.ptr == nullptr) {
                    std::cout << "(nil)\n";
                } else {
                    auto set = ldsValToSet(s);
                    auto it = set->begin();
                    for (int i = 1; it != set->end(); i++, it++) {
                        std::cout << i << ") \"" << *it << "\"\n";
                    }
                }
            }
            else if (cmd == "srem") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                auto s = dbGetVal(*ledisDb, key);
                if (s.ptr == nullptr) {
                    std::cout << "0\n";
                } else {
                    dbModifyVal(*ledisDb, key, [&val](ldsVal &v) {
                        auto set = ldsValToSet(v);
                        set->erase(val);
                    });
                    auto set = ldsValToSet(dbGetVal(*ledisDb, key));
                    std::cout << set->size() << '\n';
                }
            }
            else if (cmd == "flushdb") {
                dbFlush(*ledisDb);
                std::cout << "OK\n";
            }
            else {
                std::cout << "ERROR: Unknown command\n";
                exit(1);
            
            }
        } catch (const std::exception &e) {
            std::cout << "ERROR: " << e.what() << '\n';
        }
    }
}

// auto reader = [](int i){
//     for (size_t n{0}; n < buffer_size; ++n) {
//         random_sleep_for(50, 100);

//         std::shared_lock lck{buffer_mtx};  // shared access with other readers

//         std::cout << "Thread " << i << " reading " << buffer[n] << " from buffer[" << n << "]\n";
//         random_sleep_for(50, 100);
//         std::cout << "Thread " << i << " done reading " << buffer[n] << " from buffer[" << n << "]\n";
//     }
// };

// auto writer = [](int i){
//     for (size_t n{0}; n < buffer_size; ++n) {
//         random_sleep_for(40, 80);

//         std::unique_lock lck{buffer_mtx};  // exclusive access to buffer

//         std::cout << "\tThread " << i << " writing " << i*(n+1) << " to buffer[" << n << "]\n";
//         std::cout << "\tThread " << i << " done writing " << i*(n+1) << " to buffer[" << n << "]\n";
//     }
// };

// int main()
// {
//     std::vector<std::thread> writers{};
//     std::vector<std::thread> readers{};
//     for (int i{0}; i < 5; ++i) {
//         writers.emplace_back(writer, i + 1);
//         readers.emplace_back(reader, i + 1);
//     }
//     for (int i{0}; i < 5; ++i) {
//         writers[i].join();
//         readers[i].join();
//     }
// }
