#pragma once

#include <iostream>
#include <stdexcept>
#include <string>

#include "ldsDb.h"
#include "ldsSnapshot.h"
#include "logger.h"

extern logger LOGGER;

class dbGate {
public:
    ldsDb *ledisDb;
    ldsSnapshot *ledisSnapshot;

    dbGate() {
        ledisDb = new ldsDb{};
        ledisSnapshot = new ldsSnapshot{};
    }

    ~dbGate() {
        delete ledisDb;
        delete ledisSnapshot;
    }

    int parseAndExecute(const std::string &cmdStr, ldsRet &ret) {
        try {
            ldsCmd cmd = parseCmd(cmdStr);
            if (cmd.cmd == CMD_EXIT) {
                return -1;
            }
            ledisDb->execute(cmd, ret);

            if (ret.type == RET_UNKNOWN) {
                switch (cmd.cmd) {
                    case CMD_SNAPSHOT:
                        LOGGER.info("[COMMAND] Save");
                        if (cmd.args != nullptr && strlen(cmd.args) > 0)
                            throw std::runtime_error("Save command does not take arguments");
                        if (!ledisSnapshot->createSnapshot(*ledisDb))
                            throw std::runtime_error("Failed to create snapshot");
                        ret.type = RET_OK;
                        ret.ptr = nullptr;
                        break;
                    case CMD_RESTORE: {
                        LOGGER.info("[COMMAND] Restore");
                        auto tmpDb = ledisSnapshot->restoreSnapshot();
                        if (tmpDb != nullptr) {
                            delete ledisDb;
                            ledisDb = tmpDb;
                            ret.type = RET_OK;
                            ret.ptr = nullptr;
                        } else
                            throw std::runtime_error("Failed to restore snapshot");
                        break;
                    }
                    default:
                        throw std::runtime_error("Unknown command");
                }
            }
            ledisSnapshot->addCmd(cmd);
            return 1;

        } catch (const std::exception &e) {
            ret.type = RET_ERR;
            ret.ptr = new std::string(e.what());
            LOGGER.error("[ERROR] " + std::string(e.what()));
            return 0;
        }
    }
};