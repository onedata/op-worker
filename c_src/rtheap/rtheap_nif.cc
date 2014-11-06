/*********************************************************************
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
*********************************************************************/

#include <functional>
#include <string>
#include <iostream>

#include "nifpp.h"
#include "rtheap.h"

one::provider::rt_heap heap;

extern "C" {

static ERL_NIF_TERM push_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    try
    {
        nifpp::str_atom record_name;
        std::string file_id;
        long int offset, size, priority;
        auto record = std::make_tuple(std::ref(record_name), std::ref(file_id), std::ref(offset), std::ref(size), std::ref(priority));

        nifpp::get_throws(env, argv[0], record);
        one::provider::rt_block block(file_id, offset, size, priority);
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
        nifpp::str_atom record_name("rt_block");
        std::string file_id = "file";
        long int offset = 1, size = 2, priority = 3;
        auto record = std::make_tuple(record_name, file_id, offset, size, priority);

        return nifpp::make(env, record);
    }
    catch(nifpp::badarg) {}
    return enif_make_badarg(env);
}

static ErlNifFunc nif_funcs[] = {
    {"push", 1, push_nif},
    {"fetch", 0, fetch_nif}
};

ERL_NIF_INIT(rtheap, nif_funcs, NULL, NULL, NULL, NULL)

} //extern C