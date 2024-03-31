#pragma once

#include <iostream>
#include <string>
#include <thread>

class logger {
public:
    bool debugMode = false;
    std::string prefix = "";

    void info(const std::string &msg) const {
        std::cout << prefix << "[INFO] " << msg << std::endl;
    }

    void warning(const std::string &msg) const {
        std::cout << prefix << "[WARNING] " << msg << std::endl;
    }

    void error(const std::string &msg) const {
        std::cerr << prefix << "[ERROR] " << msg << std::endl;
    }

    void debug(const std::string &msg) const {
        if (this->debugMode)
            std::cout << prefix << "[DEBUG] " << msg << std::endl;
    }
} LOGGER{true, std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + " "};
