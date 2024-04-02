#pragma once

#include <httpserver.hpp>

#include "dbGate.h"

extern logger LOGGER;

using namespace httpserver;

class dbQueryResource : public http_resource {
private:
    dbGate *db;
public:
    explicit dbQueryResource(class dbGate *db) : db(db) {}

    std::shared_ptr<http_response> render_POST(const http_request &req) override {
        auto body = req.get_content();
        LOGGER.info("[REQUEST] Body: " + std::string(body));

        ldsRet ret{};
        db->parseAndExecute(std::string(body), ret);

        std::string resp;
        switch (ret.type) {
            case RET_STR:
                if (ret.ptr == nullptr) {
                    resp = "(nil)";
                    break;
                }
                resp = "\"" + *(std::string *) ret.ptr + "\"";
                delete (std::string *) ret.ptr;
                break;
            case RET_INT:
                if (ret.ptr == nullptr) {
                    resp = "(nil)";
                    break;
                }
                resp = "(integer) " + std::to_string(*(int *) ret.ptr);
                delete (int *) ret.ptr;
                break;
            case RET_BOOL:
                if (ret.ptr == nullptr) {
                    resp = "(nil)";
                    break;
                }
                resp = *(bool *) ret.ptr ? "1" : "0";
                delete (bool *) ret.ptr;
                break;
            case RET_OK:
                resp = "OK";
                break;
            case RET_LIST: {
                if (ret.ptr == nullptr) {
                    resp = "(empty list)";
                    break;
                }
                auto *list = (std::vector<std::string> *) ret.ptr;
                if (list->empty()) {
                    resp = "(empty list)";
                    break;
                }
                resp = "1) \"" + list->at(0) + "\"";
                for (int i = 1; i < list->size(); i++) {
                    resp += "\n" + std::to_string(i + 1) + ") \"" + list->at(i) + "\"";
                }
                delete list;
                break;
            }
            case RET_ERR:
                resp = "ERROR: " + *(std::string *) ret.ptr;
                delete (std::string *) ret.ptr;
                break;
            case RET_UNKNOWN:
                resp = "ERROR: An error occurred.";
                break;
        }

        return std::shared_ptr<http_response>(new string_response(resp));
    }

    std::shared_ptr<http_response> render(const http_request &) override {
        return std::shared_ptr<http_response>(new string_response("Invalid request method. Use POST instead."));
    }
};