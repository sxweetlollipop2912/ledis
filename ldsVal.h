#pragma once

#include <string>
#include <cassert>
#include <set>

#include "linked_list.h"

#define STRING_T 0
#define LIST_T 1
#define SET_T 2

struct ldsVal {
    void *ptr;
    unsigned type:2;
};

std::string *ldsValToStr(ldsVal val) {
    if (val.type != STRING_T) {
        throw std::runtime_error("Attempt to convert non-string value to string");
    }
    return (std::string *)val.ptr;
}

llist<std::string> *ldsValToList(ldsVal val) {
    if (val.type != LIST_T) {
        throw std::runtime_error("Attempt to convert non-list value to list");
    }
    return (llist<std::string> *)val.ptr;
}

std::set<std::string> *ldsValToSet(ldsVal val) {
    if (val.type != SET_T) {
        throw std::runtime_error("Attempt to convert non-set value to set");
    }
    return (std::set<std::string> *)val.ptr;
}
