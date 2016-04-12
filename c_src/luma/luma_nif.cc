#include "../nifpp.h"

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <rados/librados.hpp>

#include <string>

extern "C" {

ERL_NIF_TERM make_error(ErlNifEnv *env, const std::string &reason)
{
    return nifpp::make(env, std::make_tuple(nifpp::str_atom{"error"}, reason));
}

static ERL_NIF_TERM create_ceph_user_nif(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    auto user_id = nifpp::get<std::string>(env, argv[0]);
    auto mon_host = nifpp::get<std::string>(env, argv[1]);
    auto cluster_name = nifpp::get<std::string>(env, argv[2]);
    auto pool_name = nifpp::get<std::string>(env, argv[3]);
    auto ceph_admin = nifpp::get<std::string>(env, argv[4]);
    auto ceph_admin_key = nifpp::get<std::string>(env, argv[5]);

    librados::Rados cluster;
    int ret = cluster.init2(ceph_admin.c_str(), cluster_name.c_str(), 0);
    if (ret < 0)
        return make_error(env, "Couldn't initialize the cluster handle.");

    ret = cluster.conf_set("mon host", mon_host.c_str());
    if (ret < 0)
        return make_error(
            env, "Couldn't set monitor host configuration variable.");

    ret = cluster.conf_set("key", ceph_admin_key.c_str());
    if (ret < 0)
        return make_error(env, "Couldn't set key configuration variable.");

    ret = cluster.connect();
    if (ret < 0)
        return make_error(env, "Couldn't connect to cluster.");

    std::string ceph_user_name = "client." + user_id;

    // This must be done this way, any unnecessary white space
    // will cause ceph to reject JSON
    std::ostringstream cmd_stream;
    cmd_stream << "{\"prefix\":\"auth "
                  "get-or-create\",\"caps\":[\"mon\",\"allow "
                  "r\",\"osd\",\"allow rw pool="
               << pool_name << "\"],\"entity\":\"" << ceph_user_name << "\"}";
    std::string cmd = cmd_stream.str();

    ceph::bufferlist inbuf;
    ceph::bufferlist outbuf;
    std::string outs;
    ret = cluster.mon_command(cmd, inbuf, &outbuf, &outs);

    if (ret < 0)
        return make_error(env, outs);

    std::string out{outbuf.c_str()};
    std::vector<std::string> strs;
    boost::split(strs, out, boost::is_any_of("\n "));

    return nifpp::make(
        env, std::make_tuple(nifpp::str_atom{"ok"},
                 std::make_tuple(ceph_user_name, strs[strs.size() - 2])));
}

static ErlNifFunc nif_funcs[] = {{"create_ceph_user", 6, create_ceph_user_nif}};

ERL_NIF_INIT(luma_nif, nif_funcs, NULL, NULL, NULL, NULL);

} // extern C
