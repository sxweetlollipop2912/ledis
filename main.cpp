#include <chrono>
#include <iostream>  // cout
#include <thread>  // this_thread
#include <vector>

#include "ldsDb.h"

int main() {
    auto ledisDb = new ldsDb{};

    for (std::string cmd = ""; std::cin >> cmd;) {
        // turn cmd to lowercase
        for (auto &c: cmd) {
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
                ledisDb->preCommand({key});
                ledisDb->setStr(key, val);
                std::cout << "OK\n";
            } else if (cmd == "get") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto val = ledisDb->getStr(key);
                ledisDb->postAccessCommand({key});
                if (val.has_value()) {
                    std::cout << "\"" << val.value() << "\"\n";
                } else {
                    std::cout << "(nil)\n";
                }
            } else if (cmd == "llen") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto len = ledisDb->getListLen(key);
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "lpush") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                ledisDb->preCommand({key});
                auto len = ledisDb->pushList(key, val, LFRONT);
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "rpush") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                ledisDb->preCommand({key});
                auto len = ledisDb->pushList(key, val, LBACK);
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "lpop") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto val = ledisDb->popList(key, LFRONT);
                ledisDb->postAccessCommand({key});
                if (val.has_value()) {
                    std::cout << "\"" << val.value() << "\"\n";
                } else {
                    std::cout << "(nil)\n";
                }
            } else if (cmd == "rpop") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto val = ledisDb->popList(key, LBACK);
                ledisDb->postAccessCommand({key});
                if (val.has_value()) {
                    std::cout << "\"" << val.value() << "\"\n";
                } else {
                    std::cout << "(nil)\n";
                }
            } else if (cmd == "lrange") {
                std::string key;
                std::cin >> key;
                int start, stop;
                std::cin >> start >> stop;
                ledisDb->preCommand({key});
                auto vals = ledisDb->rangeList(key, start, stop);
                ledisDb->postAccessCommand({key});
                if (vals.empty()) {
                    std::cout << "(empty list)\n";
                } else {
                    for (int i = 0; i < vals.size(); i++) {
                        std::cout << i + 1 << ") \"" << vals[i] << "\"\n";
                    }
                }
            } else if (cmd == "sadd") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                ledisDb->preCommand({key});
                auto len = ledisDb->insertSet(key, {val});
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "scard") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto len = ledisDb->getSetLen(key);
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "smembers") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto vals = ledisDb->getSetMems(key);
                ledisDb->postAccessCommand({key});
                if (vals.empty()) {
                    std::cout << "(empty set)\n";
                } else {
                    for (int i = 0; i < vals.size(); i++) {
                        std::cout << i + 1 << ") \"" << vals[i] << "\"\n";
                    }
                }
            } else if (cmd == "srem") {
                std::string key;
                std::cin >> key;
                std::string val;
                std::cin >> val;
                ledisDb->preCommand({key});
                auto len = ledisDb->removeSet(key, {val});
                ledisDb->postAccessCommand({key});
                std::cout << len << '\n';
            } else if (cmd == "sinter") {
                std::string key1, key2;
                std::cin >> key1 >> key2;
                ledisDb->preCommand({key1, key2});
                auto vals = ledisDb->getSetInter({key1, key2});
                ledisDb->postAccessCommand({key1, key2});
                if (vals.empty()) {
                    std::cout << "(empty set)\n";
                } else {
                    for (int i = 0; i < vals.size(); i++) {
                        std::cout << i + 1 << ") \"" << vals[i] << "\"\n";
                    }
                }
            } else if (cmd == "keys") {
                ledisDb->preCommand({}, true);
                auto keys = ledisDb->getKeys();
                ledisDb->postAccessCommand({}, true);
                if (keys.empty()) {
                    std::cout << "(empty db)\n";
                } else {
                    for (int i = 0; i < keys.size(); i++) {
                        std::cout << i + 1 << ") \"" << keys[i] << "\"\n";
                    }
                }
            } else if (cmd == "del") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                std::cout << ledisDb->del(key) << '\n';
            } else if (cmd == "expire") {
                std::string key;
                std::cin >> key;
                int seconds;
                std::cin >> seconds;
                ledisDb->preCommand({key});
                std::cout << ledisDb->setTTL(key, seconds).value_or(-1) << '\n';
            } else if (cmd == "ttl") {
                std::string key;
                std::cin >> key;
                ledisDb->preCommand({key});
                auto ttl = ledisDb->getTTL(key);
                if (ttl.has_value()) {
                    std::cout << ttl.value() << '\n';
                } else {
                    std::cout << "-2\n";
                }
            } else if (cmd == "flushdb") {
                ledisDb->flush();
                std::cout << "OK\n";
            } else {
                std::cout << "ERROR: Unknown command\n";
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
