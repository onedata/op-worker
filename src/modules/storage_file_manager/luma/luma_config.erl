%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains utility functions for managing luma_config
%%% @end
%%%-------------------------------------------------------------------
-module(luma_config).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

-type config() :: #luma_config{}.

-type url() :: binary().
-type cache_timeout() :: non_neg_integer().
-type api_key() :: undefined | binary().

-export_type([url/0, cache_timeout/0, api_key/0, config/0]).

%% API
-export([new/3, get_timeout/1]).

%%-------------------------------------------------------------------
%% @doc
%% Returns new luma_config record. CacheTimeout is passed in minutes
%% @end
%%-------------------------------------------------------------------
-spec new(url(), cache_timeout(), api_key()) -> config().
new(URL, CacheTimeout, ApiKey) ->
    #luma_config{
        url = URL,
        cache_timeout = timer:minutes(CacheTimeout),
        api_key = ApiKey
    }.

%%-------------------------------------------------------------------
%% @doc
%% Returns value of cache_timeout field.
%%-------------------------------------------------------------------
-spec get_timeout(config() | undefined) -> undefined | cache_timeout().
get_timeout(undefined) -> undefined;
get_timeout(#luma_config{cache_timeout = Timeout}) -> Timeout.