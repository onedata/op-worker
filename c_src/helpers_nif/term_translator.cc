/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2013 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "term_translator.h"

#include "helpers/storageHelperFactory.h"

#include <utility>

namespace one
{
namespace provider
{

/*********************************************************************
*
*                      ERLANG TERM TRANSLATOR 
*
*********************************************************************/


std::string get_string(ErlNifEnv* env, ERL_NIF_TERM term)
{
    char str[MAX_STRING_SIZE];
    return enif_get_string(env, term, str, MAX_STRING_SIZE, ERL_NIF_LATIN1)
            ? str : "";
}

std::string get_atom(ErlNifEnv* env, ERL_NIF_TERM term)
{
    char str[MAX_STRING_SIZE];
    return enif_get_atom(env, term, str, MAX_STRING_SIZE, ERL_NIF_LATIN1)
            ? str : "";
}

helpers::IStorageHelper::ArgsMap get_args(ErlNifEnv* env, ERL_NIF_TERM term)
{
    helpers::IStorageHelper::ArgsMap args;

    if(enif_is_list(env, term) && !enif_is_empty_list(env, term))
    {
        int i = 0;
        ERL_NIF_TERM list, head, tail;
        for(list = term; enif_get_list_cell(env, list, &head, &tail); list = tail)
            args.emplace(helpers::srvArg(i), get_string(env, head));
    }

    return std::move(args);
}

bool is_int(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifSInt64 num;
    return enif_get_int64(env, term, &num);
}

ErlNifSInt64 get_int(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifSInt64 num;
    return enif_get_int64(env, term, &num) ? num : 0;
}

ErlNifUInt64 get_uint(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ErlNifUInt64 num;
    return enif_get_uint64(env, term, &num) ? num : 0;
}

bool check_common_args(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    if(argc < 2)
        return false;

    return is_int(env, argv[0]) && is_int(env, argv[1]);
}

struct fuse_file_info get_ffi(ErlNifEnv* env, ERL_NIF_TERM term)
{
    struct fuse_file_info ffi = {0,0,0,0,0,0,0,0,0,0};
    const ERL_NIF_TERM *elems;
    int n, i = 0;

    if(!enif_get_tuple(env, term, &n, &elems) || n != 11)
        return ffi;

    if(get_atom(env, elems[i++]) != FFI_RECORD_NAME)
        return ffi;

    ffi.flags = get_int(env, elems[i++]);
    ffi.fh_old = get_uint(env, elems[i++]);
    ffi.writepage = get_int(env, elems[i++]);
    ffi.direct_io = get_uint(env, elems[i++]);
    ffi.keep_cache = get_uint(env, elems[i++]);
    ffi.flush = get_uint(env, elems[i++]);
    ffi.nonseekable = get_uint(env, elems[i++]);
    ffi.padding = get_uint(env, elems[i++]);
    ffi.fh = get_uint(env, elems[i++]);
    ffi.lock_owner = get_uint(env, elems[i++]);

    return ffi;
}

ERL_NIF_TERM make_ffi(ErlNifEnv* env, struct fuse_file_info ffi)
{
    ERL_NIF_TERM elems[32];
    int i = 0;
    elems[i++] = enif_make_atom(env, FFI_RECORD_NAME);

    elems[i++] = enif_make_int64(env, (ErlNifSInt64)ffi.flags);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.fh_old);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)ffi.writepage);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.direct_io);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.keep_cache);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.flush);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.nonseekable);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.padding);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.fh);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)ffi.lock_owner);

    return enif_make_tuple_from_array(env, elems, i);
}

ERL_NIF_TERM make_statvfs(ErlNifEnv* env, struct statvfs stat)
{
    ERL_NIF_TERM elems[32];
    int i = 0;
    elems[i++] = enif_make_atom(env, STATVFS_RECORD_NAME);

    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_bsize);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_frsize);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_blocks);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_bfree);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_bavail);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_files);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_ffree);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_favail);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_fsid);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_flag);
    elems[i++] = enif_make_uint64(env, (ErlNifUInt64)stat.f_namemax);

    return enif_make_tuple_from_array(env, elems, i);
}

ERL_NIF_TERM make_stat(ErlNifEnv* env, struct stat st)
{
    ERL_NIF_TERM elems[32];
    int i = 0;
    elems[i++] = enif_make_atom(env, STAT_RECORD_NAME);

    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_dev);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_ino);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_mode);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_nlink);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_uid);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_gid);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_rdev);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_size);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_blksize);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_blocks);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_atime);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_mtime);
    elems[i++] = enif_make_int64(env, (ErlNifSInt64)st.st_ctime);

    return enif_make_tuple_from_array(env, elems, i);
}


} // namespace provider
} // namespace one
