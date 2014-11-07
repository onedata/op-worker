/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include <functional>
#include <string>
#include <vector>

#include "nifpp.h"
#include "rtheap.h"

using namespace one::provider;

rt_heap heap;

extern "C" {

static ERL_NIF_TERM push_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        nifpp::str_atom record_name;
        std::string file_id;
        long int offset, size, priority;
        auto record = std::make_tuple(std::ref(record_name),
                                      std::ref(file_id),
                                      std::ref(offset),
                                      std::ref(size),
                                      std::ref(priority));

        nifpp::get_throws(env, argv[0], record);
        rt_block block(file_id, offset, size, priority);
        heap.push(block);

        return nifpp::make(env, nifpp::str_atom("ok"));
    }
    catch(nifpp::badarg) {}
    return enif_make_badarg(env);
}

static ERL_NIF_TERM fetch_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        rt_block block = heap.fetch();
        auto record = std::make_tuple(nifpp::str_atom("rt_block"),
                                      block.file_id(),
                                      block.offset(),
                                      block.size(),
                                      block.priority());

        return nifpp::make(env, record);
    }
    catch(nifpp::badarg) {}
    return enif_make_badarg(env);
}

static ERL_NIF_TERM fetch_all_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        std::vector< std::tuple<nifpp::str_atom,
                           std::string,
                           long int,
                           long int,
                           int> > records;

        for(auto block : heap.fetch_all())
            records.push_back(std::make_tuple(nifpp::str_atom("rt_block"),
                                              block.file_id(),
                                              block.offset(),
                                              block.size(),
                                              block.priority()));

        return nifpp::make(env, records);
    }
    catch(nifpp::badarg) {}
    return enif_make_badarg(env);
}

static ErlNifFunc nif_funcs[] = {
    {"push", 1, push_nif},
    {"fetch", 0, fetch_nif},
    {"fetch_all", 0, fetch_all_nif},
};

ERL_NIF_INIT(rtheap, nif_funcs, NULL, NULL, NULL, NULL)

} //extern C