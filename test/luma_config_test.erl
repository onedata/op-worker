%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of what a storage update's LUMA diff changes. A change of the LUMA
%%% config rotates the storage's LUMA DB namespace, dropping every mapping, so a
%%% diff that repeats the settings the storage has must change nothing.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_config_test).
-author("Bartosz Walkowicz").

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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
external_feed_config() ->
    luma_config:new_with_external_feed(?URL, ?API_KEY).
