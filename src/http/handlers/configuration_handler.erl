%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles requests asking for current configuration of
%%% Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(configuration_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_handler).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("hackney/include/hackney_lib.hrl").

-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning current configuration of Oneprovider.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    Configuration = json_utils:encode_map(get_config()),
    NewReq = cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>}, Configuration, Req
    ),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_config() -> maps:map().
get_config() ->
    CompOpVersions = application:get_env(?APP_NAME, compatible_op_versions, []),
    CompOpVersionsBin = [list_to_binary(V) || V <- CompOpVersions],
    #{
        <<"version">> => oneprovider:get_version(),
        <<"build">> => oneprovider:get_build(),
        <<"compatibleOneproviderVersions">> => CompOpVersionsBin
    }.
