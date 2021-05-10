%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-7628 make openfaas respond to https
%%% @end
%%%-------------------------------------------------------------------
-module(atm_callback_handler).
-author("Bartosz Walkowicz").

-behaviour(cowboy_handler).

-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/logging.hrl").

-export([init/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req, State) ->
    MethodBin = cowboy_req:method(Req),
    Method = binary_to_atom(string:lowercase(MethodBin), utf8),

    Path = cowboy_req:path(Req),
    {ok, Body, _} = cowboy_req:read_body(Req),

    ?error("TASK FINISHED: ~p ~p~n~p", [Method, Path, Body]),

    {ok, cowboy_req:reply(?HTTP_200_OK, Req), State}.
