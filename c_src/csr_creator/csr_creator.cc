/*********************************************************************
*  @author Krzysztof Trzepla
*  @copyright (C): 2014 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*  @end
**********************************************************************
*  @doc This is an implementation of Erlang NIF library described
*  in csr_creator_nif interface. CSR fields are set to default values
*  which than will be overwritten by Global Registry.
*  @end
*********************************************************************/

#include <botan/init.h>
#include <botan/auto_rng.h>
#include <botan/x509self.h>
#include <botan/rsa.h>
#include <botan/dsa.h>
#include <iostream>
#include <fstream>
#include <memory>

using namespace Botan;

#define KEY_SIZE 2048

int create_csr(char* password, char* key_path, char* csr_path)
{
    Botan::LibraryInitializer init;
    try
    {
        AutoSeeded_RNG rng;
        RSA_PrivateKey priv_key(rng, KEY_SIZE);
        std::ofstream key_file(key_path);
        key_file << PKCS8::PEM_encode(priv_key, rng, std::string(password));

        X509_Cert_Options opts;

        // default values for certificate fields, which will be later overwritten by Global Registry
        opts.common_name = "common name";
        opts.country = "PL";
        opts.organization = "organization";
        opts.email = "email";

        PKCS10_Request req = X509::create_cert_req(opts, priv_key, "SHA-256", rng);

        std::ofstream req_file(csr_path);
        req_file << req.PEM_encode();
    }
    catch(std::exception& e)
    {
        std::cout << e.what() << std::endl;
        return 1;
    }
    return 0;
}
