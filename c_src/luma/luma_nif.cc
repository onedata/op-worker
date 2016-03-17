#include "nifpp.h"
#include <string>

#include <rados/librados.hpp>
#include <boost/algorithm/string.hpp>

extern "C" {

static ERL_NIF_TERM create_ceph_user_nif(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    try {
        std::string user_id;
        std::string mon_host;
        std::string cluster_name;
        std::string pool_name;
        std::string ceph_admin;
        std::string ceph_admin_key;

        nifpp::get_throws(env, argv[0], user_id);
        nifpp::get_throws(env, argv[1], mon_host);
        nifpp::get_throws(env, argv[2], cluster_name);
        nifpp::get_throws(env, argv[3], pool_name);
        nifpp::get_throws(env, argv[4], ceph_admin);
        nifpp::get_throws(env, argv[5], ceph_admin_key);

        librados::Rados cluster;
        int ret = cluster.init2(ceph_admin.c_str(), cluster_name.c_str(), 0);
        if (ret < 0)
            return enif_make_tuple2(env, enif_make_atom(env, "error"),
                enif_make_string(env, "Couldn't initialize the cluster handle.",
                                        ERL_NIF_LATIN1));

        ret = cluster.conf_set("mon host", mon_host.c_str());
        if (ret < 0)
            return enif_make_tuple2(
                env, enif_make_atom(env, "error"),
                enif_make_string(env,
                    "Couldn't set monitor host configuration variable.",
                    ERL_NIF_LATIN1));

        ret = cluster.conf_set("key", ceph_admin_key.c_str());
        if (ret < 0)
            return enif_make_tuple2(
                env, enif_make_atom(env, "error"),
                enif_make_string(env,
                    "Couldn't set key configuration variable.",
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
                   << pool_name << "\"],\"entity\":\"" << ceph_user_name
                   << "\"}";
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
    catch (nifpp::badarg) {
        return enif_make_badarg(env);
    }
}

static ErlNifFunc nif_funcs[] = {{"create_ceph_user", 6, create_ceph_user_nif}};

ERL_NIF_INIT(luma_nif, nif_funcs, NULL, NULL, NULL, NULL);

} // extern C