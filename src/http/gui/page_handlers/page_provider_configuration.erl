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

-define(to_binaries(__List), [list_to_binary(V) || V <- __List]).

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
    ProviderId = oneprovider:get_id_or_undefined(),
    Name = case provider_logic:get_name() of
        {ok, N} -> N;
        _ -> undefined
    end,
    Domain = case provider_logic:get_domain() of
        {ok, D} -> D;
        _ -> undefined
    end,
    OnezoneDomain = case ProviderId of
        undefined -> undefined;
        _ -> list_to_binary(oneprovider:get_oz_domain())
    end,
    CompOzVersions = application:get_env(?APP_NAME, compatible_oz_versions, []),
    CompOpVersions = application:get_env(?APP_NAME, compatible_op_versions, []),
    CompOcVersions = application:get_env(?APP_NAME, compatible_oc_versions, []),

    #{
        <<"providerId">> => gs_protocol:undefined_to_null(ProviderId),
        <<"name">> => gs_protocol:undefined_to_null(Name),
        <<"domain">> => gs_protocol:undefined_to_null(Domain),
        <<"onezoneDomain">> => gs_protocol:undefined_to_null(OnezoneDomain),
        <<"version">> => oneprovider:get_version(),
        <<"build">> => oneprovider:get_build(),
        <<"compatibleOnezoneVersions">> => ?to_binaries(CompOzVersions),
        <<"compatibleOneproviderVersions">> => ?to_binaries(CompOpVersions),
        <<"compatibleOneclientVersions">> => ?to_binaries(CompOcVersions)
    }.
