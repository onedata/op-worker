#!/usr/bin/env escript
%%! -name create_storage@test_env

main([Cookie, Node, Name, AuthUrl, ContainerName, TenantName, UserName, Password]) ->
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    NodeAtom = list_to_atom(Node),
    Helper = safe_call(NodeAtom, fslogic_storage, new_helper_init, [<<"Swift">>, #{
        <<"auth_url">> => list_to_binary(AuthUrl), <<"container_name">> => list_to_binary(ContainerName),
        <<"tenant_name">> => list_to_binary(TenantName), <<"user_name">> => list_to_binary(UserName),
        <<"password">> => list_to_binary(Password)}]),
    Storage = safe_call(NodeAtom, fslogic_storage, new_storage, [list_to_binary(Name), [Helper]]),
    safe_call(NodeAtom, storage, create, [Storage]).

safe_call(Node, Module, Function, Args) ->
    case rpc:call(Node, Module, Function, Args) of
        {badrpc, X} ->
            io:format(standard_error, "ERROR: in module ~p:~n {badrpc, ~p} in rpc:call(~p, ~p, ~p, ~p).~n",
                [?MODULE, X, Node, Module, Function, Args]),
            halt(42);
        {error, X} ->
            io:format(standard_error, "ERROR: in module ~p:~n {error, ~p} in rpc:call(~p, ~p, ~p, ~p).~n",
                [?MODULE, X, Node, Module, Function, Args]),
            halt(42);
        X ->
            X
    end.
