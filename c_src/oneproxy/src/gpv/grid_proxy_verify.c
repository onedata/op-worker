/*********************************************************************
*  @author Rafal Slota
*  @author Konrad Zemek
*  @copyright (C): 2013-2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "grid_proxy_verify.h"

#include <globus/globus_gsi_callback.h>
#include <globus/globus_gsi_callback_constants.h>
#include <openssl/x509_vfy.h>
#include <openssl/x509v3.h>

#include <stdlib.h>

static void gpv_deactivate(void) {
    EVP_cleanup(); // inverse of OpenSSL_add_all_algorithms
    CRYPTO_cleanup_all_ex_data();
    globus_module_deactivate(GLOBUS_GSI_CALLBACK_MODULE);
}

static void gpv_activate(void) {
    static int activated = 0;
    if(activated)
        return;

    activated = 1;
    OpenSSL_add_all_algorithms();
    globus_module_activate(GLOBUS_GSI_CALLBACK_MODULE);
    atexit(gpv_deactivate);
}

gpv_status gpv_init(GPV_CTX *gpv_ctx) {
    gpv_activate();

    gpv_ctx->last_error = 0;

    gpv_ctx->x509_context = X509_STORE_CTX_new();
    gpv_ctx->trusted_store = X509_STORE_new();
    gpv_ctx->chain_stack = sk_X509_new_null();
    gpv_ctx->proxy_cert = NULL;
    gpv_ctx->verify_param = X509_VERIFY_PARAM_new();

    if(!gpv_ctx->x509_context || !gpv_ctx->trusted_store ||
       !gpv_ctx->chain_stack || !gpv_ctx->verify_param)
        return GPV_MEMORY_ALLOCATE_ERROR;

    gpv_ctx->callback_data = NULL;
    globus_gsi_callback_data_init(&gpv_ctx->callback_data);
    globus_gsi_callback_get_X509_STORE_callback_data_index(&gpv_ctx->callback_data_index);
    globus_gsi_callback_set_allow_missing_signing_policy(gpv_ctx->callback_data, 1);

    return GPV_SUCCESS;
}

void gpv_cleanup(GPV_CTX *gpv_ctx) {
    if(gpv_ctx->proxy_cert) X509_free(gpv_ctx->proxy_cert);
    if(gpv_ctx->trusted_store) X509_STORE_free(gpv_ctx->trusted_store);
    if(gpv_ctx->chain_stack) sk_X509_pop_free(gpv_ctx->chain_stack, X509_free);
    if(gpv_ctx->x509_context) X509_STORE_CTX_free(gpv_ctx->x509_context);
    if(gpv_ctx->verify_param) X509_VERIFY_PARAM_free(gpv_ctx->verify_param);

    if(gpv_ctx->callback_data) globus_gsi_callback_data_destroy(gpv_ctx->callback_data);
}

gpv_status gpv_verify(GPV_CTX *gpv_ctx) {
    if(!X509_VERIFY_PARAM_set_flags(gpv_ctx->verify_param,
                                    X509_V_FLAG_ALLOW_PROXY_CERTS |
                                    X509_V_FLAG_CRL_CHECK_ALL))
        return GPV_VERIFY_PARAM_SET_ERROR;

    // Initialize X509_STORE using GPV context
    if(!X509_STORE_CTX_init(gpv_ctx->x509_context, gpv_ctx->trusted_store,
                            gpv_ctx->proxy_cert, gpv_ctx->chain_stack))
        return GPV_CTX_INIT_ERROR;

    X509_VERIFY_PARAM_set_depth(gpv_ctx->verify_param, 100);
    X509_STORE_CTX_set0_param(gpv_ctx->x509_context, gpv_ctx->verify_param);
    gpv_ctx->verify_param = NULL; // the structure is now freed by OpenSSL

    gpv_ctx->x509_context->check_issued = globus_gsi_callback_check_issued;
    X509_STORE_CTX_set_ex_data(gpv_ctx->x509_context, gpv_ctx->callback_data_index,
                               (void*) gpv_ctx->callback_data);
    X509_STORE_set_verify_cb_func(gpv_ctx->x509_context,
                                  globus_gsi_callback_create_proxy_callback);

    if(!X509_verify_cert(gpv_ctx->x509_context)) {
        gpv_ctx->last_error = X509_STORE_CTX_get_error(gpv_ctx->x509_context);
        X509_STORE_CTX_cleanup(gpv_ctx->x509_context);
        return GPV_VALIDATE_ERROR;
    }

    X509_STORE_CTX_cleanup(gpv_ctx->x509_context);
    return GPV_SUCCESS;
}

int gpv_get_error(GPV_CTX *ctx) {
    return ctx->last_error;
}

gpv_status gpv_set_leaf_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    gpv_ctx->proxy_cert = d2i_X509(&gpv_ctx->proxy_cert, (const unsigned char**)&buff, len);
    return gpv_ctx->proxy_cert != NULL ? GPV_SUCCESS : GPV_CERT_READ_ERROR;
}

gpv_status gpv_add_trusted_ca(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509 *tmp = d2i_X509(NULL, (const unsigned char**)&buff, len);

    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;

    if(X509_STORE_add_cert(gpv_ctx->trusted_store, tmp) < 1) {
        X509_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }

    X509_free(tmp); // add_cert makes a copy
    return GPV_SUCCESS;
}

gpv_status gpv_add_chain_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509 *tmp = d2i_X509(NULL, (const unsigned char**)&buff, len);

    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;

    if(sk_X509_push(gpv_ctx->chain_stack, tmp) < 1) {
        X509_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }

    return GPV_SUCCESS;
}

gpv_status gpv_add_crl_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509_CRL *tmp = d2i_X509_CRL(NULL, (const unsigned char**)&buff, len);

    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;

    if(X509_STORE_add_crl(gpv_ctx->trusted_store, tmp) < 1) {
        X509_CRL_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }

    X509_CRL_free(tmp); // add_crl makes a copy
    return GPV_SUCCESS;
}
