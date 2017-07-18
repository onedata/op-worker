%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(luma_config).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

-type config() :: #luma_config{}.

-export_type([config/0]).

%% API
-export([new/3, get_timeout/1]).

%%-------------------------------------------------------------------
%% @doc
%% Returns new luma_config record. CacheTimeout is in minutes
%% @end
%%-------------------------------------------------------------------
-spec new(binary(), non_neg_integer(), binary()) -> config().
new(URL, CacheTimeout, ApiKey) ->
    #luma_config{
        url = URL,
        cache_timeout = timer:minutes(CacheTimeout),
        api_key = ApiKey
    }.

get_timeout(undefined) -> undefined;
get_timeout(#luma_config{cache_timeout = Timeout}) -> Timeout.