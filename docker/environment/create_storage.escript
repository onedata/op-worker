#!/usr/bin/env escript
%%! -name create_storage@test_env -setcookie cookie2

main([Node, Name, Path]) ->
  try
    Helper = rpc:call(list_to_atom(Node), fslogic_storage, new_helper_init, [<<"DirectIO">>, #{<<"root_path">> => list_to_binary(Path)}]),
    Storage = rpc:call(list_to_atom(Node), fslogic_storage, new_storage, [list_to_binary(Name), [Helper]]),
    rpc:call(list_to_atom(Node), storage, create, [Storage])
    %% io:format("OK!")
  catch
    T:M -> io:format("~p ~p~n", [T, M])
  end.

