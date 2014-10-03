/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2013 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#ifndef TERM_TRANSLATOR_H
#define TERM_TRANSLATOR_H 1

#include "helpers/IStorageHelper.h"

#include <erl_nif.h>
#include <fuse.h>
#include <fcntl.h>
#include <unistd.h>

#include <vector>
#include <string>

#define MAX_STRING_SIZE 2048

// Names of Erlang records used in C code
#define FFI_RECORD_NAME         "st_fuse_file_info"
#define STATVFS_RECORD_NAME     "st_statvfs"
#define STAT_RECORD_NAME        "st_stat"

namespace one
{
namespace provider
{

std::string get_string(ErlNifEnv* env, ERL_NIF_TERM term);                      // Term to string
std::string get_atom(ErlNifEnv* env, ERL_NIF_TERM term);                        // Term to atom (as string)
helpers::IStorageHelper::ArgsMap get_args(ErlNifEnv* env, ERL_NIF_TERM term);   // Term to ArgsMap
bool is_int(ErlNifEnv* env, ERL_NIF_TERM term);                                 // Checks if term is an int
ErlNifSInt64 get_int(ErlNifEnv* env, ERL_NIF_TERM term);                        // Term to int64
ErlNifUInt64 get_uint(ErlNifEnv* env, ERL_NIF_TERM term);                       // Term to uint64
bool check_common_args(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);    // Checks if name of storage helper and its arguments were passed in argv
struct fuse_file_info get_ffi(ErlNifEnv* env, ERL_NIF_TERM term);               // Term to struct fuse_file_info
ERL_NIF_TERM make_ffi(ErlNifEnv* env, struct fuse_file_info ffi);               // struct fuse_file_info to erlang term
ERL_NIF_TERM make_statvfs(ErlNifEnv* env, struct statvfs stat);                 // struct statvfs to erlang term
ERL_NIF_TERM make_stat(ErlNifEnv* env, struct stat st);                         // struct stat to erlang term

} // namespace provider
} // namespace one

#endif
