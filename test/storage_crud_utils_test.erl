%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of what the pretty print of a storage spec, which a create and an
%%% update log at info level, leaves of its secrets. Besides the helper
%%% credentials, a spec carries the API key of an external LUMA feed, and an
%%% update that changes only the LUMA config carries no credentials at all.
%%% They also cover the normalization of the QoS parameters that a create
%%% and an update start with.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_crud_utils_test).
-author("Bartosz Walkowicz").

-include("modules/storage/helpers/helpers.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(API_KEY, <<"luma-feed-api-key-1234">>).


%%%===================================================================
%%% Tests
%%%===================================================================


create_spec_hides_luma_api_key_test() ->
    Printed = pretty_print(#storage_create_spec{
        type = <<"posix">>,
        name = <<"some-storage">>,
        luma = external_luma_spec(?API_KEY),
        credentials = #posix_helper_credentials{uid = 0},
        configuration = #posix_helper_configuration{mount_point = <<"/mnt/st">>}
    }),

    ?assertEqual(nomatch, string:find(Printed, ?API_KEY)),
    ?assertNotEqual(nomatch, string:find(Printed, ?CONFIDENTIAL_MASK)).


update_spec_hides_luma_api_key_test() ->
    Printed = pretty_print(#storage_update_spec{
        type = <<"posix">>,
        luma = external_luma_spec(?API_KEY),
        credentials = #posix_helper_credentials_diff{uid = 0}
    }),

    ?assertEqual(nomatch, string:find(Printed, ?API_KEY)).


%% An update of the LUMA config alone has no credentials diff, and the key
%% must be masked on that path as well.
update_spec_without_credentials_hides_luma_api_key_test() ->
    Printed = pretty_print(#storage_update_spec{
        type = <<"posix">>,
        luma = external_luma_spec(?API_KEY),
        credentials = undefined
    }),

    ?assertEqual(nomatch, string:find(Printed, ?API_KEY)).


%% A mask in place of a key that isn't there would claim one in the log.
absent_luma_api_key_is_not_masked_test() ->
    Printed = pretty_print(#storage_update_spec{
        type = <<"posix">>,
        luma = #luma_spec{feed = auto}
    }),

    ?assertEqual(nomatch, string:find(Printed, ?CONFIDENTIAL_MASK)).


create_spec_qos_parameters_are_normalized_test() ->
    #storage_create_spec{qos_parameters = QosParameters} = storage_crud_utils:normalize_qos_parameters(
        #storage_create_spec{type = <<"posix">>, qos_parameters = #{<<"priority">> => <<"2">>}}
    ),

    ?assertEqual(#{<<"priority">> => 2}, QosParameters).


update_spec_qos_parameters_are_normalized_test() ->
    #storage_update_spec{qos_parameters = QosParameters} = storage_crud_utils:normalize_qos_parameters(
        #storage_update_spec{type = <<"posix">>, qos_parameters = #{<<"priority">> => <<"2">>}}
    ),

    ?assertEqual(#{<<"priority">> => 2}, QosParameters).


%% The QoS parameters of an update replace all the previous ones, so an update
%% that doesn't name them must keep them undefined.
update_spec_without_qos_parameters_keeps_them_undefined_test() ->
    #storage_update_spec{qos_parameters = QosParameters} = storage_crud_utils:normalize_qos_parameters(
        #storage_update_spec{type = <<"posix">>}
    ),

    ?assertEqual(undefined, QosParameters).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
pretty_print(Spec) ->
    lists:flatten(storage_crud_utils:pretty_print_spec(Spec)).


%% @private
external_luma_spec(ApiKey) ->
    #luma_spec{feed = external, url = <<"https://luma.example.com">>, api_key = ApiKey}.
