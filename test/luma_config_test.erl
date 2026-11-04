%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of luma_config: which LUMA diffs of a storage update change the
%%% config, and which are refused.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_config_test).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/errors.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(URL, <<"https://luma.example.com">>).
-define(API_KEY, <<"some-api-key">>).


%%%===================================================================
%%% Tests
%%%===================================================================


empty_diff_of_external_feed_changes_nothing_test() ->
    ?assertEqual({error, no_update}, luma_config:update(external_feed_config(), #{})).


repeated_settings_of_external_feed_change_nothing_test() ->
    ?assertEqual({error, no_update}, luma_config:update(external_feed_config(), #{url => ?URL})),
    ?assertEqual(
        {error, no_update},
        luma_config:update(external_feed_config(), #{feed => external, url => ?URL, api_key => ?API_KEY})
    ).


new_url_of_external_feed_is_an_update_test() ->
    NewUrl = <<"https://other-luma.example.com">>,

    {ok, NewConfig} = luma_config:update(external_feed_config(), #{url => NewUrl}),

    ?assertEqual(NewUrl, luma_config:get_url(NewConfig)),
    ?assertEqual(?API_KEY, luma_config:get_api_key(NewConfig)).


%% Every request to the feed would go nowhere.
empty_url_of_external_feed_is_refused_test() ->
    ?assertMatch(?ERR_BAD_VALUE_EMPTY(_, url), luma_config:update(external_feed_config(), #{url => <<>>})),
    ?assertMatch(
        ?ERR_BAD_VALUE_EMPTY(_, url),
        luma_config:update(luma_config:new(auto), #{feed => external, url => <<>>})
    ).


%% A feed other than the external one has no URL, so an empty one is dropped
%% with the rest of the external feed's settings.
empty_url_with_a_switch_off_the_external_feed_is_not_refused_test() ->
    {ok, NewConfig} = luma_config:update(external_feed_config(), #{feed => auto, url => <<>>}),

    ?assertEqual(auto, luma_config:get_feed(NewConfig)),
    ?assertEqual(undefined, luma_config:get_url(NewConfig)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
external_feed_config() ->
    luma_config:new_with_external_feed(?URL, ?API_KEY).
