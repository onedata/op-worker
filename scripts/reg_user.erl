#!/usr/bin/env escript

-module(reg_user).

-define(default_cookie, veil_cluster_node).
-define(default_ccm_name, "ccm").
-define(default_worker_name, "worker").


main([HostName, UserName, UserMail, PemFile] = Args) ->
    set_up_net_kernel(),
    NodeName = list_to_atom(?default_worker_name ++ "@" ++ HostName),

    {ok, PemBin} = file:read_file(PemFile),
    Cert = public_key:pem_decode(PemBin),
    [Leaf | Chain] = [public_key:pkix_decode_cert(DerCert, otp) || {'Certificate', DerCert, _} <- Cert],
    

    call(NodeName, fun() ->  
        case dao_lib:apply(dao_users, exist_user, [{login, UserName}], 1) of
            {ok, true} -> ok;
            {ok, false} ->
                {ok, EEC} = gsi_handler:find_eec_cert(Leaf, Chain, gsi_handler:is_proxy_certificate(Leaf)),
                {rdnSequence, Rdn} = gsi_handler:proxy_subject(EEC),
                {ok, DnString} = user_logic:rdn_sequence_to_dn_string(Rdn),
                user_logic:create_user(UserName, UserName, [], UserMail, [DnString])
        end
    end).

call(Node, Fun) ->
    Self = self(),
    pong = net_adm:ping(Node),
    Pid = spawn(Node, fun() -> Self ! {self(), Fun()} end),
    receive
        {Pid, Ans} ->
            Ans
    after 10000 ->
        {error, timeout}
    end.

set_up_net_kernel() ->
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).
