#include "erl_nif.h"
#include <string>

#include <rados/librados.hpp>
#include <boost/algorithm/string.hpp>

#define MAX_STRING_SIZE 2048

extern int create_ceph_user(char *user_id, char *mon_host, char *cluster_name,
    char *pool_name, char *ceph_admin, char *ceph_admin_key);

static ERL_NIF_TERM create_ceph_user_nif(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    char user_id[MAX_STRING_SIZE];
    char mon_host[MAX_STRING_SIZE];
    char cluster_name[MAX_STRING_SIZE];
    char pool_name[MAX_STRING_SIZE];
    char ceph_admin[MAX_STRING_SIZE];
    char ceph_admin_key[MAX_STRING_SIZE];

    if (argc != 6)
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[0], user_id, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[1], mon_host, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[2], cluster_name, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[3], pool_name, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[4], ceph_admin, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    if (!enif_get_string(
            env, argv[5], ceph_admin_key, MAX_STRING_SIZE, ERL_NIF_LATIN1))
        return enif_make_badarg(env);

    librados::Rados cluster;
    int ret = cluster.init2(ceph_admin, cluster_name, 0);
    if (ret < 0)
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
            enif_make_string(env, "Couldn't initialize the cluster handle.",
                                    ERL_NIF_LATIN1));

    ret = cluster.conf_set("mon host", mon_host);
    if (ret < 0)
        return enif_make_tuple2(
            env, enif_make_atom(env, "error"),
            enif_make_string(env,
                "Couldn't set monitor host configuration variable.",
                ERL_NIF_LATIN1));

    ret = cluster.conf_set("key", ceph_admin_key);
    if (ret < 0)
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
            enif_make_string(env, "Couldn't set key configuration variable.",
                                    ERL_NIF_LATIN1));

    ret = cluster.connect();
    if (ret < 0)
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
            enif_make_string(env, "Couldn't connect to cluster.",
                                    ERL_NIF_LATIN1));

    std::ostringstream user_name_stream;
    user_name_stream << "client." << user_id;
    std::string ceph_user_name = user_name_stream.str();

    std::ostringstream cmd_stream;
    cmd_stream << "{\"prefix\":\"auth "
                  "get-or-create\",\"caps\":[\"mon\",\"allow "
                  "r\",\"osd\",\"allow rw pool_name="
               << pool_name << "\"],\"entity\":\"" << ceph_user_name << "\"}";
    std::string cmd = cmd_stream.str();
    ceph::bufferlist inbuf;
    ceph::bufferlist outbuf;
    std::string outs;
    ret = cluster.mon_command(cmd, inbuf, &outbuf, &outs);

    if (ret < 0)
        return enif_make_tuple2(env, enif_make_atom(env, "error"),
            enif_make_string(env, outs.c_str(), ERL_NIF_LATIN1));

    std::string out{outbuf.c_str()};
    std::vector<std::string> strs;
    boost::split(strs, out, boost::is_any_of("\n "));

    return enif_make_tuple2(
        env, enif_make_atom(env, "ok"),
        enif_make_tuple2(env,
            enif_make_string(env, ceph_user_name.c_str(), ERL_NIF_LATIN1),
            enif_make_string(env, strs[strs.size() - 2].c_str(),
                             ERL_NIF_LATIN1)));
}

static ErlNifFunc nif_funcs[] = {{"create_ceph_user", 6, create_ceph_user_nif}};

ERL_NIF_INIT(luma_nif, nif_funcs, NULL, NULL, NULL, NULL);
