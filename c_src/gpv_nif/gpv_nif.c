/*********************************************************************
*  @author Rafal Slota
*  @author Konrad Zemek
*  @copyright (C): 2013-2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "erl_nif.h"
#include "grid_proxy_verify.h"


typedef gpv_status (*cert_add_fun)(GPV_CTX*, const byte*, int);

#define GPV_NIF_BAD_ARG(env, ctx) {gpv_cleanup(&ctx); return enif_make_badarg(env);}

#define GPV_NIF_ERROR_CHECK(env, ctx, result) { \
    if(result != GPV_SUCCESS) { \
        gpv_cleanup(&ctx); \
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_int(env, result)); \
    } \
}

static gpv_status add_cert(ErlNifEnv *env, GPV_CTX *ctx, ERL_NIF_TERM list, cert_add_fun fun) {
    ErlNifBinary cert;
    ERL_NIF_TERM head, tail;
    gpv_status result;

    for(; enif_get_list_cell(env, list, &head, &tail); list = tail) {
        if(!enif_inspect_binary(env, head, &cert))
            continue;

        result = (*fun)(ctx, cert.data, cert.size);
        enif_release_binary(&cert);
        GPV_NIF_ERROR_CHECK(env, *ctx, result);
    }

    return GPV_SUCCESS;
}

static gpv_status set_user_cert(ErlNifEnv *env, GPV_CTX *ctx, const ERL_NIF_TERM term) {
    ErlNifBinary cert;
    gpv_status result;

    if(!enif_inspect_binary(env, term, &cert))
        GPV_NIF_BAD_ARG(env, *ctx);

    result = gpv_set_leaf_cert(ctx, cert.data, cert.size);
    enif_release_binary(&cert);
    GPV_NIF_ERROR_CHECK(env, *ctx, result);

    return GPV_SUCCESS;
}

// verify_cert/3
/*********************************************************************
*  Main NIF callback method
*********************************************************************/
static ERL_NIF_TERM verify_cert(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    GPV_CTX ctx;
    gpv_status result;
    int i, error;

    // Init gpv_ctx
    result = gpv_init(&ctx);
    GPV_NIF_ERROR_CHECK(env, ctx, result);

    result = set_user_cert(env, &ctx, argv[0]);
    if(result != GPV_SUCCESS)
        return result;

    // Set chain certs
    cert_add_fun cert_add_fun[4] = {
        NULL, gpv_add_chain_cert, gpv_add_trusted_ca, gpv_add_crl_cert
    };
    for(i = 1; i <= 3; ++i) {
        result = add_cert(env, &ctx, argv[i], cert_add_fun[i]);
        if(result != GPV_SUCCESS)
            return result;
    }

    result = gpv_verify(&ctx);
    error = gpv_get_error(&ctx);

    gpv_cleanup(&ctx);

    if(result == GPV_SUCCESS)
        return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_int(env, 1));
    else if(result == GPV_VALIDATE_ERROR)
        return enif_make_tuple3(env, enif_make_atom(env, "ok"), enif_make_int(env, 0), enif_make_int(env, error));
    else
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_int(env, result));
}

// Match verify_cert/3 to erlang's verify_cert_c/4
static ErlNifFunc nif_funcs[] =
{
    {"verify_cert_c", 4, verify_cert}
};

ERL_NIF_INIT(gsi_nif, nif_funcs, NULL,NULL,NULL,NULL);
