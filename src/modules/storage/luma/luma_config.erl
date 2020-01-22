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

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").

-type config() :: #luma_config{}.

-type url() :: binary().
-type api_key() :: undefined | binary().

-export_type([url/0, api_key/0, config/0]).

%% API
-export([new/2, get_url/1, get_api_key/1]).

%%-------------------------------------------------------------------
%% @doc
%% Returns new luma_config record.
%% @end
%%-------------------------------------------------------------------
-spec new(url(), api_key()) -> config().
new(URL, ApiKey) ->
    #luma_config{
        url = URL,
        api_key = ApiKey
    }.

-spec get_url(config()) -> url().
get_url(#luma_config{url = LumaUrl}) ->
    LumaUrl.

-spec get_api_key(config()) -> api_key().
get_api_key(#luma_config{api_key = ApiKey}) ->
    ApiKey.
