#!/usr/bin/env escript
%%! -name create_storage@test_env

main([Cookie, Node, Name, Path]) ->
  try
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    NodeAtom = list_to_atom(Node),
    Helper = rpc:call(NodeAtom, fslogic_storage, new_helper_init, [<<"DirectIO">>, #{<<"root_path">> => list_to_binary(Path)}]),
    Storage = rpc:call(NodeAtom, fslogic_storage, new_storage, [list_to_binary(Name), [Helper]]),
    rpc:call(NodeAtom, storage, create, [Storage])
catch
     T : M -> io:format(standard_error, "~p : ~p~n", [T, M]),
     halt(42)
  end.
