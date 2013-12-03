%%%-------------------------------------------------------------------
%%% @author Michał Sitko
%%% @copyright (C): 2013 ACK CYFRONET AGH
%%% @doc
%%%
%%% @end
%%% Created : 23. Nov 2013 1:51 PM
%%%-------------------------------------------------------------------

main(_)->
  Pwd = get_pwd(),
  SourcePaths = ["cluster_elements", "manual", "veil_modules"],
  Paths = lists:map(fun(Path) -> filename:join(Pwd, Path) end, SourcePaths),
  edoc:application(veil_cluster_test, Pwd, [no_packages, {source_path, Paths}, {dir, filename:join(Pwd, "../doc/test")}]).

get_pwd() ->
  case file:get_cwd() of
    {ok, Dir} ->
      filename:join(Dir, filename:dirname(escript:script_name()));
    Error ->
      io:format("Cannot file:get_cwd: ~p~n", [Error])
  end.