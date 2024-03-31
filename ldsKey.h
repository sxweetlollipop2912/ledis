#pragma once

#include <chrono>
#include <string>
#include <list>

#include "ldsVal.h"

struct ldsKey {
    std::string key;
    std::list<ldsVal>::iterator val_iter;
    std::optional<std::chrono::time_point<std::chrono::system_clock>> ttl = std::nullopt;
};
