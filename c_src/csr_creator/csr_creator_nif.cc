/*********************************************************************
*  @author Krzysztof Trzepla
*  @copyright (C): 2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*  @end
**********************************************************************
*  @doc This is an interface for Erlang NIF library. It contains one
*  method that allows to create private key and Certificate Signing
*  Request using Botan library.
*  @end
*********************************************************************/

#include "erl_nif.h"

#define MAX_STRING_SIZE 2048

extern int create_csr(char* password, char* key_path, char* csr_path);

ERL_NIF_TERM create_csr_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc != 3)
    {
        return enif_make_badarg(env);
    }

    int ret;
    char password[MAX_STRING_SIZE];
    char key_path[MAX_STRING_SIZE];
    char csr_path[MAX_STRING_SIZE];

    if (!enif_get_string(env, argv[0], password, MAX_STRING_SIZE, ERL_NIF_LATIN1))
    {
	    return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[1], key_path, MAX_STRING_SIZE, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }
    if (!enif_get_string(env, argv[2], csr_path, MAX_STRING_SIZE, ERL_NIF_LATIN1))
    {
        return enif_make_badarg(env);
    }

    ret = create_csr(password, key_path, csr_path);

    return enif_make_int(env, ret);
}

ErlNifFunc nif_funcs[] = {
    {"create_csr", 3, create_csr_nif}
};

ERL_NIF_INIT(csr_creator, nif_funcs, NULL, NULL, NULL, NULL)
