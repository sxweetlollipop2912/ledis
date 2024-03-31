#pragma once

#include <list>
#include <shared_mutex>
#include <unistd.h>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <set>
#include <fstream>
#include <iostream>

#include "ldsCmd.h"
#include "logger.h"

extern logger LOGGER;

#define SNAPSHOT_FILENAME "ledis"
#define SNAPSHOT_EXT ".snpsht"

const std::set MODIFIABLE_COMMANDS = {CMD_SSET, CMD_LPUSH, CMD_RPUSH, CMD_LPOP, CMD_RPOP, CMD_SADD, CMD_SREM, CMD_GDEL,
                                      CMD_GFLUSHDB};

static std::string getCurrentDateTime() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm *timeinfo = std::localtime(&now_time);

    std::ostringstream oss;
    oss << std::put_time(timeinfo, "%H_%M_%S_%d_%m_%Y");
    return oss.str();
}


class ldsSnapshot {
private:
    std::list<ldsCmd> cmds;
    std::shared_mutex cmds_mtx;
    std::shared_mutex file_mtx;

#define ULOCK(lck, mtx) std::unique_lock<std::shared_mutex> lck{mtx}
#define SLOCK(lck, mtx) std::shared_lock<std::shared_mutex> lck{mtx}

    /* Preconditions: unique lock on cmds_mtx */
    void clear() {
        for (auto &cmd: cmds) {
            if (cmd.args != nullptr) {
                delete cmd.args;
                cmd.args = nullptr;
            }
        }
        cmds.clear();
    }

public:
    void addCmd(ldsCmd cmd) {
        if (MODIFIABLE_COMMANDS.find(cmd.cmd) == MODIFIABLE_COMMANDS.end()) {
            return;
        }
        ULOCK(lck, cmds_mtx);
        if (cmd.cmd == CMD_GFLUSHDB) {
            LOGGER.info("[SNAPSHOT] Flush log");
            this->clear();
        } else {
            LOGGER.info("[SNAPSHOT] Add to log: " + std::to_string(cmd.cmd) + " " + std::string{cmd.args});
            cmds.push_back(cmd);
        }
    }

    bool createSnapshot(ldsDb &db) {
        auto writeCmd = [&](std::ofstream &of, ldsCmd &cmd) {
            LOGGER.info("[SNAPSHOT] Write to file: " + std::to_string(cmd.cmd) + " " + std::string{cmd.args});
            of.write(reinterpret_cast<const char *>(&cmd.cmd), sizeof(ldsCmd::cmd));
            size_t len = strlen(cmd.args);
            of.write(reinterpret_cast<const char *>(&len), sizeof(size_t)); // Write the length
            of.write(cmd.args, len);
        };

        std::string filename = std::string{SNAPSHOT_FILENAME} + SNAPSHOT_EXT;
        std::string tmp_filename = getCurrentDateTime() + SNAPSHOT_EXT;
        auto rc = fork();
        if (rc < 0) return false;

        if (rc == 0) {
            std::ofstream of(tmp_filename, std::ios::out | std::ios::binary | std::ios::trunc);
            if (!of.is_open()) exit(1);

            // Snapshot all TTL at this current time point
            std::list<ldsCmd> expires;

            for (auto &cmd: cmds) {
                auto key = parseArgs(cmd.args)[0];
                auto ttl = db.cmdTTL(key);
                if (ttl < -1) continue;
                if (ttl > -1) {
                    expires.push_back({CMD_GEXPIRE, strdup((key + " " + std::to_string(ttl)).c_str())});
                }
                writeCmd(of, cmd);
            }
            for (auto &cmd: expires) {
                writeCmd(of, cmd);
                delete cmd.args;
                cmd.args = nullptr;
            }
            of.close();
            exit(0);
        }

        int status;
        waitpid(rc, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            return false;
        }

        // rename old snapshot to back up, rename new snapshot, then remove back up
        ULOCK(lck, file_mtx);
        if (access(filename.c_str(), F_OK) == 0) {
            if (rename(filename.c_str(), (std::string{filename} + ".bak").c_str()) != 0) {
                return false;
            }
        }
        if (rename(tmp_filename.c_str(), filename.c_str()) != 0) {
            return false;
        }
        remove((std::string{filename} + ".bak").c_str());

        return true;
    }

    ldsDb *restoreSnapshot() {
        auto readCmd = [&](std::ifstream &ifile, ldsCmd &cmd) -> bool {
            if (!ifile.read(reinterpret_cast<char *>(&cmd.cmd), sizeof(ldsCmd::cmd))) return false;
            size_t len;
            if (!ifile.read(reinterpret_cast<char *>(&len), sizeof(size_t))) return false;
            char *args = new char[len + 1];
            if (!ifile.read(args, len)) return false;
            args[len] = '\0';
            cmd.args = args;
            LOGGER.info("[SNAPSHOT] Read from file: " + std::to_string(cmd.cmd) + " " + std::string{cmd.args});
            return true;
        };

        std::string filename = std::string{SNAPSHOT_FILENAME} + SNAPSHOT_EXT;
        ULOCK(lck_file, file_mtx);
        if (access(filename.c_str(), F_OK) != 0) {
            return nullptr;
        }

        std::ifstream ifile(filename, std::ios::in | std::ios::binary);
        if (!ifile.is_open()) return nullptr;

        ldsCmd cmd{};
        std::list<ldsCmd> cmds;
        auto *db = new ldsDb();
        while (readCmd(ifile, cmd)) {
            cmds.push_back(cmd);
            ldsRet ret{};
            db->execute(cmd, ret);
        }
        ifile.close();

        ULOCK(lck_cmds, cmds_mtx);
        this->clear();
        std::copy(cmds.begin(), cmds.end(), std::back_inserter(this->cmds));
        return db;
    }
};
