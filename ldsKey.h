#pragma once

#include <chrono>
#include <string>

#include "linked_list.h"
#include "ldsVal.h"

struct ldsKey {
    std::string key;
    llist<ldsVal>::iterator val_iter;
    std::optional<std::chrono::time_point<std::chrono::system_clock>> ttl = std::nullopt;
    bool zombie = false;
};
