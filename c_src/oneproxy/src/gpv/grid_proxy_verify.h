/*********************************************************************
*  @author Rafal Slota
*  @author Konrad Zemek
*  @copyright (C): 2013-2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef GRID_PROXY_VERIFY_H
#define GRID_PROXY_VERIFY_H

#include <globus/globus_gsi_callback.h>
#include <openssl/x509_vfy.h>

// Error codes
#define GPV_SUCCESS                     0
#define GPV_MEMORY_ALLOCATE_ERROR       1
#define GPV_VERIFY_PARAM_SET_ERROR      2
#define GPV_CERT_READ_ERROR             3
#define GPV_CERT_INSERT_ERROR           4
#define GPV_CTX_INIT_ERROR              5
#define GPV_VALIDATE_ERROR              6

// Types
typedef char byte;
typedef unsigned int gpv_status;

// GPV Context type
typedef struct {
    X509_STORE_CTX                  *x509_context;
    X509_STORE                      *trusted_store;
    STACK_OF(X509)                  *chain_stack;
    X509                            *proxy_cert;
    X509_VERIFY_PARAM               *verify_param;

    globus_gsi_callback_data_t      callback_data;
    int                             callback_data_index; // bonus CTX data index
    int                             last_error;
} GPV_CTX;

// Initializes static data used by globus and openssl
gpv_status gpv_init(GPV_CTX*);

// Unloads all data globally allocated by globus and openssl
void gpv_cleanup(GPV_CTX*);

// Gets last globus/openssl error code
int gpv_get_error(GPV_CTX*);

// Proceed with verification
gpv_status gpv_verify(GPV_CTX*);

// Set peer certificate
gpv_status gpv_set_leaf_cert(GPV_CTX*, const byte*, int);

// Add trusted CA certificate to CA store
gpv_status gpv_add_trusted_ca(GPV_CTX*, const byte*, int);

// Add peer additional not-trusted chain certificate
gpv_status gpv_add_chain_cert(GPV_CTX*, const byte*, int);

// Add CRL certificate to CRL store
gpv_status gpv_add_crl_cert(GPV_CTX*, const byte*, int);


#endif // GRID_PROXY_VERIFY_H
