%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Basic wrapper functions for easier mocking
%%% @end
%%%-------------------------------------------------------------------
-module(wrapper).
-author("Tomasz Lichon").

%% API
-export([read_file/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv file:read_file(Filename)
%% @end
%%--------------------------------------------------------------------
-spec read_file(Filename) -> {ok, Binary} | {error, Reason} when
    Filename :: file:name_all(),
    Binary :: binary(),
    Reason :: file:posix() | badarg | terminated | system_limit.
read_file(Filename) ->
    file:read_file(Filename).

%%%===================================================================
%%% Internal functions
%%%===================================================================