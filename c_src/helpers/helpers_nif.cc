#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION

#include "helpers/storageHelperFactory.h"
#include "directIOHelper.h"

#include "nifpp.h"

#include <pwd.h>
#include <grp.h>

#ifndef __APPLE__
#include <sys/fsuid.h>
#endif

namespace one {
namespace helpers {
namespace enif {
    std::shared_ptr<communication::Communicator> nullCommunicator = nullptr;
    BufferLimits limits = BufferLimits();
    asio::io_service dio_service;
    asio::io_service cproxy_service;

    StorageHelperFactory SHFactory = StorageHelperFactory(nullCommunicator,  limits, dio_service, cproxy_service);

}
}
}

using std::string;
using namespace nifpp;


/// RAII FS user UID/GID holder class
class UserCTX
{
public:

    UserCTX(ErlNifEnv* env, ERL_NIF_TERM uidTerm, ERL_NIF_TERM gidTerm)
    {
        uid_t uid = -1;
        gid_t gid = -1;

        if(!get(env, uidTerm, uid)) {
            uid = uNameToUID(get<string>(env, uidTerm));
        }

        if(!get(env, gidTerm, gid)) {
            gid = gNameToGID(get<string>(env, gidTerm));
        }

        initCTX(uid, gid);
    }

    ~UserCTX()
    {
// Only to make compilation possible - helpers_nif does NOT support platforms other then Linux
#ifndef __APPLE__
        setfsuid(0);
        setfsgid(0);
#endif
    }

    uid_t uid()
    {
        return m_uid;
    }

    gid_t gid()
    {
       return m_gid;
    }

private:
    uid_t   m_uid;
    gid_t   m_gid;

    void initCTX(uid_t uid, gid_t gid)
    {
        m_uid = uid;
        m_gid = gid;

// Only to make compilation possible - helpers_nif does NOT support platforms other then Linux
#ifndef __APPLE__
        if(uid != 0) {
            setgroups(0, nullptr);
            setegid(-1);
            seteuid(-1);
        }

        setfsuid(m_uid);
        setfsgid(m_gid);
#endif
    }

    uid_t uNameToUID(std::string uname)
    {
        struct passwd *ownerInfo = getpwnam(uname.c_str()); // Static buffer, do NOT free !
        return (ownerInfo ? ownerInfo->pw_uid : -1);
    }

    gid_t gNameToGID(std::string gname, std::string uname = "")
    {
        struct passwd *ownerInfo = getpwnam(uname.c_str()); // Static buffer, do NOT free !
        struct group  *groupInfo = getgrnam(gname.c_str()); // Static buffer, do NOT free !

        gid_t primary_gid = (ownerInfo ? ownerInfo->pw_gid : -1);
        return (groupInfo ? groupInfo->gr_gid : primary_gid);
    }

};


void handle_result(ErlNifEnv* env, ErlNifPid pid, one::helpers::future_t<void> &f)
{
    try {
        f.get();
        enif_send(env, &pid, env, make(env, 0));
    } catch(std::system_error &e) {
        enif_send(env, &pid, env, make(env, e.code().value()));
    }
}

#define BADARG enif_make_badarg(env)


/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in helpers_nif.erl
*
*********************************************************************/

resource_ptr<one::helpers::IStorageHelper> get_helper(ErlNifEnv* env, ERL_NIF_TERM term)
{
    resource_ptr<one::helpers::IStorageHelper> helper;
    get(env, term, helper);
    return std::move(helper);
}

static ERL_NIF_TERM sh_link(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UserCTX(env, argv[1], argv[2]);
    auto sh = get_helper(env, argv[0]);
    auto pid = get<ErlNifPid>(env, argv[3]);

    auto future = sh->ash_link(get<string>(env, argv[4]).c_str(), get<string>(env, argv[5]).c_str());
    future.then([=](one::helpers::future_t<void> f) {
        handle_result(env, pid, f);
    });

    return make(env, str_atom("ok"));
}

//static ERL_NIF_TERM sh_getattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct stat st;
//    int ret = sh->sh_getattr(auto sh = (env, argv[4]).c_str(), &st);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_stat(env, st));
//}
//
//static ERL_NIF_TERM sh_access(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_access(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_mknod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_mknod(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), get<int>(env, argv[6])));
//}
//
//static ERL_NIF_TERM sh_unlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_unlink(auto sh = (env, argv[4]).c_str()));
//}
//
//static ERL_NIF_TERM sh_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_rename(get<string>(env, argv[4]).c_str(), auto sh = (env, argv[5]).c_str()));
//}
//
//static ERL_NIF_TERM sh_chmod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_chmod(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_chown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_chown(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), get<int>(env, argv[6])));
//}
//
//static ERL_NIF_TERM sh_chown_name(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    uid_t uid = -1;
//    gid_t gid = -1;
////
////    struct passwd *ownerInfo = getpwnam(aget<string>(env, argv[5]).c_str()); // Static buffer, do NOT free !
////    struct group  *groupInfo = getgrnam(auto sh = (env, argv[6]).c_str()); // Static buffer, do NOT free !
////
////    if(!ownerInfo && auto sh = (env, argv[5]).size() > 0) // User not found
////        return enif_make_int(env, -EINVAL);
////    if(!groupInfo && auto sh = (env, argv[6]).size() > 0) // Group not found
////        return enif_make_int(env, -EINVAL);
////
////    uid   = (ownerInfo ? ownerInfo->pw_uid : -1);
////    gid   = (groupInfo ? groupInfo->gr_gid : -1);
//
//    return make(env, sh->sh_chown(get<string>(env, argv[4]).c_str(), uid, gid));
//}
//
//static ERL_NIF_TERM sh_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_truncate(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}
//
//static ERL_NIF_TERM sh_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[5]);
//    ffi.flags |= O_NOFOLLOW;
//    int ret = sh->sh_open(auto sh = (env, argv[4]).c_str(), &ffi);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_ffi(env, ffi));
//}
//
//static ERL_NIF_TERM sh_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[7]);
//    unsigned int size = get<int>(env, argv[5]);
//    ERL_NIF_TERM bin;
//    char *buff = new char[size];
//
//    int ret = sh->sh_read(auto sh = (env, argv[4]).c_str(), buff, size, get<int>(env, argv[6]), &ffi);
//    char *tmp = (char *) enif_make_new_binary(env, (ret > 0 ? ret : 0), &bin);
//    memcpy(tmp, buff, (ret > 0 ? ret : 0));
//
//    delete[] buff;
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), bin);
//}
//
//static ERL_NIF_TERM sh_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[7]);
//    ErlNifBinary bin;
//    if(!enif_inspect_binary(env, argv[5], &bin))
//        return BADARG;
//
//    return make(env, sh->sh_write(get<string>(env, argv[4]).c_str(), (const char*)bin.data, bin.size, get<int>(env, argv[6]), &ffi));
//}
//
//static ERL_NIF_TERM sh_statfs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct statvfs stat;
//    int ret = sh->sh_statfs(auto sh = (env, argv[4]).c_str(), &stat);
//
//    return enif_make_tuple2(env, enif_make_int(env, ret), make_statvfs(env, stat));
//}
//
//static ERL_NIF_TERM sh_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[5]);
//    return make(env, sh->sh_release(get<string>(env, argv[4]).c_str(), &ffi));
//}
//
//static ERL_NIF_TERM sh_fsync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    struct fuse_file_info ffi = get<ffi>(env, argv[6]);
//
//    return make(env, sh->sh_fsync(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5]), &ffi));
//}
//
//static ERL_NIF_TERM sh_mkdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
//{
//    auto sh = get_helper(env, argv[0]);
//
//    return make(env, sh->sh_mkdir(get<string>(env, argv[4]).c_str(), get<int>(env, argv[5])));
//}

static ERL_NIF_TERM sh_rmdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    UserCTX(env, argv[1], argv[2]);
    auto sh = get_helper(env, argv[0]);
    auto pid = get<ErlNifPid>(env, argv[3]);

    auto future = sh->ash_rmdir(get<string>(env, argv[4]).c_str());
    future.then([=](one::helpers::future_t<void> f) {
        handle_result(env, pid, f);
    });

    return make(env, str_atom("ok"));
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    return !(register_resource<one::helpers::DirectIOHelper>(env, "helper", "DirectIO"));
}


static ErlNifFunc nif_funcs[] =
{
    {"link",        6, sh_link},
//    {"getattr",     5, sh_getattr},
//    {"access",      6, sh_access},
//    {"mknod",       7, sh_mknod},
//    {"unlink",      5, sh_unlink},
//    {"rename",      6, sh_rename},
//    {"chmod",       6, sh_chmod},
//    {"chown",       7, sh_chown},
//    {"chown_name",  7, sh_chown_name},
//    {"truncate",    6, sh_truncate},
//    {"open",        6, sh_open},
//    {"read",        8, sh_read},
//    {"write",       8, sh_write},
//    {"statfs",      5, sh_statfs},
//    {"release",     6, sh_release},
//    {"fsync",       7, sh_fsync},
//    {"mkdir",       6, sh_mkdir},
    {"rmdir",       5, sh_rmdir}
};


ERL_NIF_INIT(helpers_nif, nif_funcs, &load, NULL, NULL, NULL);
