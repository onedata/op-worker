#!/usr/bin/env escript
%%! -name gettoken@test -setcookie cookie3

main([GR_Node, UID]) ->
  try
    Token = rpc:call(list_to_atom(GR_Node), auth_logic, gen_token, [list_to_binary(UID)]),
    io:format(Token),
    {ok, File} = file:open("token", [write]),
    ok = file:write(File, Token),
    ok = file:close(File)
  catch
    T:M -> io:format("ERROR: {~p, ~p}~n", [M])
end.