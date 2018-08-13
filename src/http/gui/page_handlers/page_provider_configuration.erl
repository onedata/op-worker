%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when provider configuration page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_provider_configuration).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("global_definitions.hrl").

-export([handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(new_gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        json_utils:encode(get_config()),
        Req
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_config() -> maps:map().
get_config() ->
    ProviderId = case oneprovider:get_id_or_undefined() of
        undefined -> <<"not registered yet">>;
        Id -> Id
    end,
    CompOpVersions = application:get_env(?APP_NAME, compatible_op_versions, []),
    CompOpVersionsBin = [list_to_binary(V) || V <- CompOpVersions],
    #{
        <<"providerId">> => ProviderId,
        <<"version">> => oneprovider:get_version(),
        <<"build">> => oneprovider:get_build(),
        <<"compatibleOneproviderVersions">> => CompOpVersionsBin
    }.
