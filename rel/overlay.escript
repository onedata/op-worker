#!/usr/bin/env escript
%% -*- erlang -*-

%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This script allows to template files. It reads config containing
%%% substitution variables as Erlang terms and a template file <name>.template.
%%% It creates target file <name> where each occurrence of a pattern
%%% '{{variable}}' is replaced with the value provided in the config file.
%%% @end
%%%--------------------------------------------------------------------

%% API
-export([main/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

main([ConfigFile, TemplateFile]) ->
    {ok, Config} = file:consult(ConfigFile),
    {ok, Template} = file:read_file(TemplateFile),
    Target = lists:foldl(fun({Key, Value}, Acc) ->
        Expr = <<"{{", (erlang:atom_to_binary(Key, utf8))/binary, "}}">>,
        re:replace(Acc, Expr, Value, [global])
    end, Template, Config),
    TargetFile = filename:rootname(TemplateFile),
    file:write_file(TargetFile, Target).
