%%%--------------------------------------------------------------------
%%% @author Michał Sitko
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Generate ct doc
%%% @end
%%%--------------------------------------------------------------------

main(_) ->
    Pwd = get_pwd(),
    edoc:application(oneprovider_test, Pwd, [no_packages, {dir, filename:join(Pwd, "../doc/test_distributed")}]).

get_pwd() ->
    case file:get_cwd() of
        {ok, Dir} ->
            filename:join(Dir, filename:dirname(escript:script_name()));
        Error ->
            io:format("Cannot file:get_cwd: ~p~n", [Error])
    end.