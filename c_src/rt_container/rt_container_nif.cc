/**
 * @file rt_container_nif.cc
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#include "nifpp.h"
#include "rt_map.h"
#include "rt_exception.h"
#include "rt_container.h"
#include "rt_priority_queue.h"

#include <functional>
#include <string>
#include <vector>
#include <set>
#include <memory>

using namespace one::provider;

static int load(ErlNifEnv *env, void **priv, ERL_NIF_TERM load_info)
{
    nifpp::register_resource<std::shared_ptr<rt_container>>(env, nullptr,
                                                            "rt_container");
    return 0;
}

static ERL_NIF_TERM init_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
{
    try {
        nifpp::str_atom type;
        ErlNifUInt64 block_size;
        nifpp::get_throws(env, argv[0], type);
        nifpp::get_throws(env, argv[1], block_size);
        nifpp::resource_ptr<std::shared_ptr<rt_container>> container;
        if (type == nifpp::str_atom("priority_queue"))
            container
                = nifpp::construct_resource<std::shared_ptr<rt_container>>(
                    new rt_priority_queue(block_size));
        else if (type == nifpp::str_atom("map"))
            container
                = nifpp::construct_resource<std::shared_ptr<rt_container>>(
                    new rt_map(block_size));
        else
            return nifpp::make(
                env,
                std::make_tuple(nifpp::str_atom("error"),
                                nifpp::str_atom("unsupported_container_type")));

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                nifpp::make(env, container)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM push_nif(ErlNifEnv *env, int argc,
                             const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<std::shared_ptr<rt_container>> container;
        nifpp::str_atom record_name;
        std::string file_id;
        nifpp::TERM provider_ref;
        ErlNifUInt64 offset, size;
        int priority;
        std::list<nifpp::TERM> terms;
        auto record = std::make_tuple(std::ref(record_name), std::ref(file_id),
                                      std::ref(provider_ref), std::ref(offset),
                                      std::ref(size), std::ref(priority),
                                      std::ref(terms));

        nifpp::get_throws(env, argv[0], container);
        nifpp::get_throws(env, argv[1], record);

        rt_block block(file_id, provider_ref, offset, size, priority, terms);
        (*container)->push(block);
        ErlNifUInt64 container_size = (*container)->size();

        return nifpp::make(
            env, std::make_tuple(nifpp::str_atom("ok"), container_size));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM fetch_nif_1(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<std::shared_ptr<rt_container>> container;
        nifpp::get_throws(env, argv[0], container);

        rt_block block = (*container)->fetch();
        ErlNifUInt64 container_size = (*container)->size();
        auto record = std::make_tuple(
            nifpp::str_atom("rt_block"), block.file_id(), block.provider_ref(),
            block.offset(), block.size(), block.priority(), block.terms());

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                container_size, record));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ERL_NIF_TERM fetch_nif_3(ErlNifEnv *env, int argc,
                                const ERL_NIF_TERM argv[])
{
    try {
        nifpp::resource_ptr<std::shared_ptr<rt_container>> container;
        ErlNifUInt64 offset, size;
        nifpp::get_throws(env, argv[0], container);
        nifpp::get_throws(env, argv[1], offset);
        nifpp::get_throws(env, argv[2], size);

        std::list<std::tuple<nifpp::str_atom, std::string, nifpp::TERM,
                             ErlNifUInt64, ErlNifUInt64, int,
                             std::list<nifpp::TERM>>> records;

        for (const auto &block : (*container)->fetch(offset, size))
            records.push_back(
                std::make_tuple(nifpp::str_atom("rt_block"), block.file_id(),
                                block.provider_ref(), block.offset(),
                                block.size(), block.priority(), block.terms()));
        ErlNifUInt64 container_size = (*container)->size();

        return nifpp::make(env, std::make_tuple(nifpp::str_atom("ok"),
                                                container_size, records));
    }
    catch (const rt_exception &ex) {
        std::string message = ex.what();
        return nifpp::make(env, std::make_tuple(nifpp::str_atom("error"),
                                                nifpp::str_atom(message)));
    }
    catch (...) {
        return enif_make_badarg(env);
    }
}

static ErlNifFunc nif_funcs[] = {{"init_nif", 2, init_nif},
                                 {"push_nif", 2, push_nif},
                                 {"fetch_nif", 1, fetch_nif_1},
                                 {"fetch_nif", 3, fetch_nif_3}};

ERL_NIF_INIT(rt_container, nif_funcs, load, NULL, NULL, NULL)
