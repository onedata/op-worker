/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2013 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "erl_nif.h"
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/fsuid.h>
#include <pwd.h>
#include <grp.h>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "helpers/IStorageHelper.h"
#include "helpers/storageHelperFactory.h"

#include "term_translator.h"

#define BADARG enif_make_badarg(env)
#define INIT    if(!check_common_args(env, argc, argv)) \
                    return BADARG; \
                boost::shared_ptr<IStorageHelper> sh = SHFactory.getStorageHelper(get_string(env, argv[2]), get_str_vector(env, argv[3])); \
                if(!sh) \
                    return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "unknown_storage_helper")); \
                UserCTX holder(get_string(env, argv[0]), get_string(env, argv[1])); \
                if(holder.uid() == (uid_t)-1 || holder.gid() == (gid_t)-1) \
                    return enif_make_int(env, -EINVAL);

using namespace std;
using namespace boost;
using namespace veil::cluster;
using namespace veil::helpers;

namespace veil {
namespace cluster {

/// RAII FS user UID/GID holder class
class UserCTX {
public:
    UserCTX(uid_t uid, gid_t gid) {
        initCTX(uid, gid);
    }

    UserCTX(std::string uname, std::string gname) {
        initCTX(uname, gname);
    }

    ~UserCTX() {
        setfsuid(0);
        setfsgid(0);
    }

    uid_t uid() {
        return m_uid;
    }

    gid_t gid() {
       return m_gid;
    }

private:
    uid_t   m_uid;
    gid_t   m_gid;

    void initCTX(uid_t uid, gid_t gid) {
        m_uid = uid;
        m_gid = gid;

        setegid(-1);
        setfsuid(m_uid);
        setfsgid(m_gid);
    }

    void initCTX(std::string uname, std::string gname) {
         struct passwd *ownerInfo = getpwnam(uname.c_str()); // Static buffer, do NOT free !
         struct group  *groupInfo = getgrnam(gname.c_str()); // Static buffer, do NOT free !

         uid_t uid = (ownerInfo ? ownerInfo->pw_uid : -1);
         gid_t primary_gid = (ownerInfo ? ownerInfo->pw_gid : -1);
         gid_t gid = (groupInfo ? groupInfo->gr_gid : primary_gid);

         initCTX(uid, gid);
    }

};

StorageHelperFactory SHFactory;     // StorageHelperFactory instance

} // cluster
} // veil

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in veilhelpers_nif.erl
*
*********************************************************************/

static ERL_NIF_TERM sh_getattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct stat st;
    int ret = sh->sh_getattr(get_string(env, argv[4]).c_str(), &st);
    
    return enif_make_tuple2(env, enif_make_int(env, ret), make_stat(env, st));
}

static ERL_NIF_TERM sh_access(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]))
        return BADARG;

    return enif_make_int(env, sh->sh_access(get_string(env, argv[4]).c_str(), get_int(env, argv[5])));
}

static ERL_NIF_TERM sh_mknod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]) || !is_int(env, argv[6]))
        return BADARG;

    return enif_make_int(env, sh->sh_mknod(get_string(env, argv[4]).c_str(), get_int(env, argv[5]), get_int(env, argv[6])));
}

static ERL_NIF_TERM sh_unlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_unlink(get_string(env, argv[4]).c_str()));
}

static ERL_NIF_TERM sh_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_rename(get_string(env, argv[4]).c_str(), get_string(env, argv[5]).c_str()));
}

static ERL_NIF_TERM sh_chmod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]))
        return BADARG;

    return enif_make_int(env, sh->sh_chmod(get_string(env, argv[4]).c_str(), get_int(env, argv[5])));
}

static ERL_NIF_TERM sh_chown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]) || !is_int(env, argv[6]))
        return BADARG;

    return enif_make_int(env, sh->sh_chown(get_string(env, argv[4]).c_str(), get_int(env, argv[5]), get_int(env, argv[6])));
}

static ERL_NIF_TERM sh_chown_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    uid_t uid = -1;
    gid_t gid = -1;

    struct passwd *ownerInfo = getpwnam(get_string(env, argv[5]).c_str()); // Static buffer, do NOT free !
    struct group  *groupInfo = getgrnam(get_string(env, argv[6]).c_str()); // Static buffer, do NOT free !

    if(!ownerInfo && get_string(env, argv[5]).size() > 0) // User not found
        return enif_make_int(env, -EINVAL);
    if(!groupInfo && get_string(env, argv[6]).size() > 0) // Group not found
        return enif_make_int(env, -EINVAL);

    uid   = (ownerInfo ? ownerInfo->pw_uid : -1);
    gid   = (groupInfo ? groupInfo->gr_gid : -1);

    return enif_make_int(env, sh->sh_chown(get_string(env, argv[4]).c_str(), uid, gid));
}

static ERL_NIF_TERM sh_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]))
        return BADARG;

    return enif_make_int(env, sh->sh_truncate(get_string(env, argv[4]).c_str(), get_int(env, argv[5])));
}

static ERL_NIF_TERM sh_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;
  
    struct fuse_file_info ffi = get_ffi(env, argv[5]);
    int ret = sh->sh_open(get_string(env, argv[4]).c_str(), &ffi);

    return enif_make_tuple2(env, enif_make_int(env, ret), make_ffi(env, ffi));
}

static ERL_NIF_TERM sh_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]) || !is_int(env, argv[6]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[7]);
    unsigned int size = get_int(env, argv[5]);
    ERL_NIF_TERM bin;
    char *buff = new char[size];

    int ret = sh->sh_read(get_string(env, argv[4]).c_str(), buff, size, get_int(env, argv[6]), &ffi);
    char *tmp = (char *) enif_make_new_binary(env, (ret > 0 ? ret : 0), &bin);
    memcpy(tmp, buff, (ret > 0 ? ret : 0));

    delete[] buff;

    return enif_make_tuple2(env, enif_make_int(env, ret), bin);
}

static ERL_NIF_TERM sh_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[6]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[7]);
    ErlNifBinary bin;
    if(!enif_inspect_binary(env, argv[5], &bin))
        return BADARG;

    return enif_make_int(env, sh->sh_write(get_string(env, argv[4]).c_str(), (const char*)bin.data, bin.size, get_int(env, argv[6]), &ffi));
}

static ERL_NIF_TERM sh_statfs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct statvfs stat;
    int ret = sh->sh_statfs(get_string(env, argv[4]).c_str(), &stat);

    return enif_make_tuple2(env, enif_make_int(env, ret), make_statvfs(env, stat));
}

static ERL_NIF_TERM sh_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct fuse_file_info ffi = get_ffi(env, argv[5]);
    return enif_make_int(env, sh->sh_release(get_string(env, argv[4]).c_str(), &ffi));
}

static ERL_NIF_TERM sh_fsync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[6]);

    return enif_make_int(env, sh->sh_fsync(get_string(env, argv[4]).c_str(), get_int(env, argv[5]), &ffi));
}

static ERL_NIF_TERM sh_mkdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[5]))
        return BADARG;

    return enif_make_int(env, sh->sh_mkdir(get_string(env, argv[4]).c_str(), get_int(env, argv[5])));
}

static ERL_NIF_TERM sh_rmdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_rmdir(get_string(env, argv[4]).c_str()));
}

static ErlNifFunc nif_funcs[] =
{
    {"getattr",     5, sh_getattr},
    {"access",      6, sh_access},
    {"mknod",       7, sh_mknod},
    {"unlink",      5, sh_unlink},
    {"rename",      6, sh_rename},
    {"chmod",       6, sh_chmod},
    {"chown",       7, sh_chown},
    {"chown_name",  7, sh_chown_name},
    {"truncate",    6, sh_truncate},
    {"open",        6, sh_open},
    {"read",        8, sh_read},
    {"write",       8, sh_write},
    {"statfs",      5, sh_statfs},
    {"release",     6, sh_release},
    {"fsync",       7, sh_fsync},
    {"mkdir",       6, sh_mkdir},
    {"rmdir",       5, sh_rmdir}
};


ERL_NIF_INIT(veilhelpers_nif, nif_funcs, NULL,NULL,NULL,NULL);
