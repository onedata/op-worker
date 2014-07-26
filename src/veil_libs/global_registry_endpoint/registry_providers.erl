%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2014 01:05
%%%-------------------------------------------------------------------
-module(registry_providers).
-author("RoXeon").

-include_lib("ctool/include/logging.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

%% API
-export([get_provider_id/0, get_provider_info/1]).


get_provider_info(ProviderId) ->
    case global_registry:provider_request(get, "provider/" ++ binary_to_list(ProviderId)) of
        {ok, Data} ->
            ?info("ProviderData: ~p", [Data]),
            {ok, Data};
        {error, Reason} ->
            {error, Reason}
    end.


get_provider_id() ->
    case global_registry:provider_request(get, "provider") of
        {ok, #{<<"providerId">> := ProviderId}} ->
            {ok, ProviderId};
        {error, Reason} ->
            {error, Reason}
    end.


