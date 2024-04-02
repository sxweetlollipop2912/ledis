#pragma once

#include <string>
#include <cstring>

#define CMD_SSET 0
#define CMD_SGET 1
#define CMD_LLEN 2
#define CMD_LPUSH 3
#define CMD_RPUSH 4
#define CMD_LPOP 5
#define CMD_RPOP 6
#define CMD_LRANGE 7
#define CMD_SADD 8
#define CMD_SREM 9
#define CMD_SMEMBERS 10
#define CMD_SINTER 11
#define CMD_SCARD 12
#define CMD_GDEL 13
#define CMD_GEXPIRE 14
#define CMD_GTTL 15
#define CMD_GKEYS 16
#define CMD_GFLUSHDB 17
#define CMD_EXIT 18
#define CMD_SNAPSHOT 19
#define CMD_RESTORE 20

struct ldsCmd {
    unsigned short cmd;
    char *args;
};

#define RET_STR 0
#define RET_INT 1
#define RET_LIST 2
#define RET_BOOL 3
#define RET_OK 4
#define RET_ERR 5
#define RET_UNKNOWN 6

struct ldsRet {
    void *ptr;
    unsigned short type;
};


ldsCmd parseCmd(const std::string &line) {
    // split line into command and arguments
    std::istringstream iss{line};
    std::string cmd;
    iss >> cmd;
    std::string args;
    std::getline(iss, args);

    // convert command to lowercase
    for (auto &c: cmd) {
        c = std::tolower(c);
    }

    // parse command
    ldsCmd lds_cmd{};
    if (cmd == "set")
        lds_cmd.cmd = CMD_SSET;
    else if (cmd == "get")
        lds_cmd.cmd = CMD_SGET;
    else if (cmd == "llen")
        lds_cmd.cmd = CMD_LLEN;
    else if (cmd == "lpush")
        lds_cmd.cmd = CMD_LPUSH;
    else if (cmd == "rpush")
        lds_cmd.cmd = CMD_RPUSH;
    else if (cmd == "lpop")
        lds_cmd.cmd = CMD_LPOP;
    else if (cmd == "rpop")
        lds_cmd.cmd = CMD_RPOP;
    else if (cmd == "lrange")
        lds_cmd.cmd = CMD_LRANGE;
    else if (cmd == "sadd")
        lds_cmd.cmd = CMD_SADD;
    else if (cmd == "srem")
        lds_cmd.cmd = CMD_SREM;
    else if (cmd == "smembers")
        lds_cmd.cmd = CMD_SMEMBERS;
    else if (cmd == "sinter")
        lds_cmd.cmd = CMD_SINTER;
    else if (cmd == "scard")
        lds_cmd.cmd = CMD_SCARD;
    else if (cmd == "del")
        lds_cmd.cmd = CMD_GDEL;
    else if (cmd == "expire")
        lds_cmd.cmd = CMD_GEXPIRE;
    else if (cmd == "ttl")
        lds_cmd.cmd = CMD_GTTL;
    else if (cmd == "keys")
        lds_cmd.cmd = CMD_GKEYS;
    else if (cmd == "flushdb")
        lds_cmd.cmd = CMD_GFLUSHDB;
    else if (cmd == "exit")
        lds_cmd.cmd = CMD_EXIT;
    else if (cmd == "save")
        lds_cmd.cmd = CMD_SNAPSHOT;
    else if (cmd == "restore")
        lds_cmd.cmd = CMD_RESTORE;
    else
        throw std::runtime_error("Unknown command: " + cmd);

    lds_cmd.args = strdup(args.c_str());
    return lds_cmd;
}

std::vector<std::string> parseArgs(char *args) {
    if (args == nullptr) {
        return {};
    }
    auto arg_str = std::string{args};

    // arguments are separated by spaces
    std::istringstream iss{arg_str};
    std::vector<std::string> parsed_args;
    std::string arg;
    while (iss >> arg) {
        parsed_args.push_back(arg);
    }
    return parsed_args;
}
