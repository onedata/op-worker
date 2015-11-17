#!/usr/bin/env escript
%%! -name create_storage@test_env -setcookie cookie2

main([Node, Name, Path]) ->
  NodeAtom = list_to_atom(Node),
  Helper = rpc:call(NodeAtom, fslogic_storage, new_helper_init, [<<"DirectIO">>, #{<<"root_path">> => list_to_binary(Path)}]),
  Storage = rpc:call(NodeAtom, fslogic_storage, new_storage, [list_to_binary(Name), [Helper]]),
  rpc:call(NodeAtom, storage, create, [Storage]).
