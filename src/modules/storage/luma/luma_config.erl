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
-include_lib("ctool/include/errors.hrl").

%% API
-export([new/1, new_with_external_feed/2]).
-export([get_feed/1, get_url/1, get_api_key/1, describe/1]).
-export([set_feed/2, update/2]).

-record(luma_config, {
    feed = ?AUTO_FEED :: feed(),
    url :: undefined | url(),
    api_key :: undefined | api_key()
}).

-type config() :: #luma_config{}.
-type feed() :: ?AUTO_FEED | ?LOCAL_FEED | ?EXTERNAL_FEED.
-type url() :: binary().
-type api_key() :: undefined | binary().

%% @formatter:off
-type diff() :: #{
    feed => feed(),
    url => url(),
    api_key => api_key()
}.
%% @formatter:on


-export_type([url/0, api_key/0, config/0, feed/0, diff/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(feed()) -> config().
new(Feed) ->
    #luma_config{feed = Feed}.

-spec new_with_external_feed(url(), api_key()) -> config().
new_with_external_feed(URL, ApiKey) ->
    #luma_config{
        feed = ?EXTERNAL_FEED,
        url = URL,
        api_key = ApiKey
    }.

-spec get_feed(config()) -> feed().
get_feed(#luma_config{feed = Feed}) ->
    Feed.

-spec get_url(config()) -> url().
get_url(#luma_config{url = LumaUrl}) ->
    LumaUrl.

-spec get_api_key(config()) -> api_key().
get_api_key(#luma_config{api_key = ApiKey}) ->
    ApiKey.

-spec set_feed(config(), feed()) -> config().
set_feed(LumaConfig, Feed) ->
    LumaConfig#luma_config{feed = Feed}.


-spec update(config(), diff()) -> {ok, config()} | {error, term()}.
update(LumaConfig = #luma_config{
    feed = Feed,
    url = Url,
    api_key = ApiKey
}, Diff) when is_map(Diff) ->
    NewFeed = ensure_atom(maps:get(feed, Diff, Feed)),
    FeedHasChanged = NewFeed =/= Feed,
    case {FeedHasChanged, NewFeed =:= ?EXTERNAL_FEED} of
        {false, false} ->
            {error, no_update};
        {false, true} ->
            {ok, LumaConfig#luma_config{
                url = maps:get(url, Diff, Url),
                api_key = maps:get(api_key, Diff, ApiKey)
            }};
        {true, false} ->
            {ok, #luma_config{
                feed = NewFeed,
                url = undefined,
                api_key = undefined
            }};
        {true, true} ->
            case maps:get(url, Diff, undefined) of
                undefined ->
                    ?ERROR_MISSING_REQUIRED_VALUE(url);
                NewUrl when is_binary(NewUrl) ->
                    {ok, #luma_config{
                        feed = NewFeed,
                        url = NewUrl,
                        api_key = maps:get(api_key, Diff, ApiKey)
                    }}
            end
    end.

-spec describe(config()) -> json_utils:json_map().
describe(#luma_config{
    feed = ?EXTERNAL_FEED,
    url = LumaUrl,
    api_key = ApiKey
}) ->
    #{
        <<"lumaFeed">> => ?EXTERNAL_FEED,
        <<"lumaFeedUrl">> => LumaUrl,
        <<"lumaFeedApiKey">> => utils:undefined_to_null(ApiKey)
    };
describe(#luma_config{feed = LumaFeed}) ->
    #{
        <<"lumaFeed">> => LumaFeed
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_atom(feed() | binary()) -> feed().
ensure_atom(Atom) when is_atom(Atom) -> Atom;
ensure_atom(Binary) when is_binary(Binary) -> binary_to_existing_atom(Binary, utf8).