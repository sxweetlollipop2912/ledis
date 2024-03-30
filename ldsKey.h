#pragma once

#include <chrono>
#include <string>

#include "linked_list.h"
#include "ldsVal.h"

struct ldsKey {
    std::string key;
    llist<ldsVal>::iterator val_iter;
    std::chrono::system_clock::time_point updated_at;
};
