/* Copyright (c) 2015, Google Inc.
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "../crypto/test/scoped_types.h"
#include "internal.h"


static const struct argument kArguments[] = {
    {
     "-nprimes", kOptionalArgument,
     "The number of primes to generate (default: 2)",
    },
    {
     "-bits", kOptionalArgument,
     "The number of bits in the modulus (default: 2048)",
    },
    {
     "", kOptionalArgument, "",
    },
};

bool GenerateRSAKey(const std::vector<std::string> &args) {
  std::map<std::string, std::string> args_map;

  if (!ParseKeyValueArguments(&args_map, args, kArguments)) {
    PrintUsage(kArguments);
    return false;
  }

  unsigned bits, nprimes = 0;
  if (!GetUnsigned(&bits, "-bits", 2048, args_map) ||
      !GetUnsigned(&nprimes, "-nprimes", 2, args_map)) {
    PrintUsage(kArguments);
    return false;
  }

  ScopedRSA rsa(RSA_new());
  ScopedBIGNUM e(BN_new());
  ScopedBIO bio(BIO_new_fp(stdout, BIO_NOCLOSE));

  if (!BN_set_word(e.get(), RSA_F4) ||
      !RSA_generate_multi_prime_key(rsa.get(), bits, nprimes, e.get(), NULL) ||
      !PEM_write_bio_RSAPrivateKey(bio.get(), rsa.get(), NULL /* cipher */,
                                   NULL /* key */, 0 /* key len */,
                                   NULL /* password callback */,
                                   NULL /* callback arg */)) {
    ERR_print_errors_fp(stderr);
    return false;
  }

  return true;
}
