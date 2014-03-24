/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2013 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "grid_proxy_verify.h"
#include <stdio.h>
#include <globus/globus_gsi_callback.h>
#include <openssl/x509_vfy.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <globus/globus_gsi_callback_constants.h>


void gpv_activate() {
    OpenSSL_add_all_algorithms();
    globus_module_activate(GLOBUS_GSI_CALLBACK_MODULE);
}

void gpv_deactivate() {
    globus_module_deactivate(GLOBUS_GSI_CALLBACK_MODULE);
}

gpv_status gpv_init(GPV_CTX *gpv_ctx) {
    static int activated = 0;
    if(!activated) {
        gpv_activate();
        activated = 1;
    }

    gpv_ctx->sslContext = NULL;
    gpv_ctx->ssl = NULL;

    gpv_ctx->callback_data = NULL;

    gpv_ctx->x509_context = X509_STORE_CTX_new();
    gpv_ctx->trusted_store = X509_STORE_new();
    gpv_ctx->chain_stack = sk_X509_new_null();
    gpv_ctx->trusted_stack = sk_X509_new_null();
    gpv_ctx->crl_stack = sk_X509_CRL_new_null();
    gpv_ctx->proxy_cert = NULL;

    globus_gsi_callback_data_init(&gpv_ctx->callback_data);
    globus_gsi_callback_get_X509_STORE_callback_data_index(&gpv_ctx->callback_data_index);
    globus_gsi_callback_set_allow_missing_signing_policy(gpv_ctx->callback_data, 1);

    if(!gpv_ctx->x509_context || !gpv_ctx->trusted_store || !gpv_ctx->trusted_stack || !gpv_ctx->chain_stack || !gpv_ctx->crl_stack)
        return GPV_MEMORY_ALLOCATE_ERROR;

    return GPV_SUCCESS;
}

void gpv_cleanup(GPV_CTX *gpv_ctx) {
    // Free whole context

    if(gpv_ctx->proxy_cert) X509_free(gpv_ctx->proxy_cert);
    if(gpv_ctx->trusted_store) X509_STORE_free(gpv_ctx->trusted_store);
    if(gpv_ctx->chain_stack) sk_X509_pop_free(gpv_ctx->chain_stack, X509_free);
    if(gpv_ctx->trusted_stack) sk_X509_pop_free(gpv_ctx->trusted_stack, X509_free);
    if(gpv_ctx->crl_stack) sk_X509_CRL_pop_free(gpv_ctx->crl_stack, X509_CRL_free);

    if(gpv_ctx->x509_context) X509_STORE_CTX_free(gpv_ctx->x509_context);

    if(gpv_ctx->callback_data) globus_gsi_callback_data_destroy(gpv_ctx->callback_data);

    if(gpv_ctx->ssl) SSL_free(gpv_ctx->ssl);
    if(gpv_ctx->sslContext) SSL_CTX_free(gpv_ctx->sslContext);
}

gpv_status gpv_verify(GPV_CTX *gpv_ctx) {

    // Initialize X509_STORE using GPV context
    X509_STORE_CTX_init(gpv_ctx->x509_context, gpv_ctx->trusted_store, gpv_ctx->proxy_cert, gpv_ctx->chain_stack);
    X509_STORE_CTX_set0_crls(gpv_ctx->x509_context, gpv_ctx->crl_stack);

    gpv_ctx->x509_context->check_issued = globus_gsi_callback_check_issued;

    X509_STORE_CTX_set_depth(gpv_ctx->x509_context, 100);
    X509_STORE_CTX_set_flags(gpv_ctx->x509_context, X509_V_FLAG_ALLOW_PROXY_CERTS);
    X509_STORE_CTX_set_flags(gpv_ctx->x509_context, X509_V_FLAG_CRL_CHECK_ALL);

    X509_STORE_CTX_set_ex_data(gpv_ctx->x509_context, gpv_ctx->callback_data_index, (void *)gpv_ctx->callback_data);

    X509_STORE_set_verify_cb_func(gpv_ctx->x509_context,
                                     globus_gsi_callback_create_proxy_callback);

    if(!X509_verify_cert(gpv_ctx->x509_context))
        return GPV_VALIDATE_ERROR;

    return GPV_SUCCESS;
}

int gpv_get_error(GPV_CTX *ctx) {
    return X509_STORE_CTX_get_error(ctx->x509_context);
}

gpv_status gpv_set_leaf_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    gpv_ctx->proxy_cert = d2i_X509(&gpv_ctx->proxy_cert, &buff, len);
    if(gpv_ctx->proxy_cert == NULL)
        return GPV_CERT_READ_ERROR;
    return GPV_SUCCESS;
}

gpv_status gpv_add_trusted_ca(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509 *tmp = NULL;
    tmp = d2i_X509(&tmp, &buff, len);
    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;
    if(X509_STORE_add_cert(gpv_ctx->trusted_store, tmp) < 1) {
        X509_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }

    if(sk_X509_push(gpv_ctx->trusted_stack, tmp) < 1) {
        X509_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }
    return GPV_SUCCESS;
}

gpv_status gpv_add_chain_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509 *tmp = NULL;
    tmp = d2i_X509(&tmp, &buff, len);
    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;
    if(sk_X509_push(gpv_ctx->chain_stack, tmp) < 1) {
        X509_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }
    return GPV_SUCCESS;
}

gpv_status gpv_add_crl_cert(GPV_CTX *gpv_ctx, const byte *buff, int len) {
    X509_CRL *tmp = NULL;
    tmp = d2i_X509_CRL(&tmp, &buff, len);
    if(tmp == NULL)
        return GPV_CERT_READ_ERROR;
    if(X509_STORE_add_crl(gpv_ctx->trusted_store, tmp) < 1) {
        X509_CRL_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }

    if(sk_X509_CRL_push(gpv_ctx->crl_stack, tmp) < 1) {
        X509_CRL_free(tmp);
        return GPV_CERT_INSERT_ERROR;
    }
    return GPV_SUCCESS;
}
