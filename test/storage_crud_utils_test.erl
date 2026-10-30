%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Unit tests of storage_crud_utils, the steps that a storage create and update
%%% share: printing the spec to the log, normalizing QoS parameters and checking
%%% the configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_crud_utils_test).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("opw_panel_contracts/include/storage/common.hrl").
-include_lib("opw_panel_contracts/include/storage/posix.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(API_KEY, <<"luma-feed-api-key-1234">>).
% the hostname as Onepanel passes it, with the port and without the scheme
-define(GLOBAL_AWS_ENDPOINT, <<"s3.amazonaws.com:443">>).


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
        luma = external_luma_spec_diff(?API_KEY),
        credentials = #posix_helper_credentials_diff{uid = 0}
    }),

    ?assertEqual(nomatch, string:find(Printed, ?API_KEY)).


%% An update of the LUMA config alone has no credentials diff, and the key
%% must be masked on that path as well.
update_spec_without_credentials_hides_luma_api_key_test() ->
    Printed = pretty_print(#storage_update_spec{
        type = <<"posix">>,
        luma = external_luma_spec_diff(?API_KEY),
        credentials = undefined
    }),

    ?assertEqual(nomatch, string:find(Printed, ?API_KEY)).


%% A mask in place of a key that isn't there would claim one in the log.
absent_luma_api_key_is_not_masked_test() ->
    Printed = pretty_print(#storage_update_spec{
        type = <<"posix">>,
        luma = #luma_spec_diff{feed = auto}
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


http_credentials_without_credentials_pass_with_type_none_test() ->
    ?assertEqual(ok, storage_crud_utils:verify_credentials(
        http_helper_spec(#{<<"credentialsType">> => <<"none">>})
    )).


missing_credential_is_refused_by_its_name_test() ->
    ?assertThrow(
        ?ERR_MISSING_REQUIRED_VALUE(_, <<"credentials">>),
        storage_crud_utils:verify_credentials(
            http_helper_spec(#{<<"credentialsType">> => <<"basic">>})
        )
    ).


unexpected_credential_is_refused_by_its_name_test() ->
    ?assertThrow(
        ?ERR_BAD_DATA(_, <<"password">>, undefined),
        storage_crud_utils:verify_credentials(http_helper_spec(#{
            <<"credentialsType">> => <<"none">>,
            <<"password">> => <<"secret">>
        }))
    ).


%% A credential's value may be a secret, and the error goes back to the
%% client and to the log.
invalid_credential_is_refused_without_its_value_test() ->
    ?assertThrow(
        ?ERR_BAD_DATA(_, <<"credentials">>, undefined),
        storage_crud_utils:verify_credentials(http_helper_spec(#{
            <<"credentialsType">> => <<"basic">>,
            <<"credentials">> => <<"null">>
        }))
    ).


global_aws_endpoint_without_region_is_refused_test() ->
    ?assertThrow(
        ?ERR_MISSING_REQUIRED_VALUE(_, <<"region">>),
        storage_crud_utils:verify_s3_region(s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{}))
    ).


%% Onepanel's model takes an empty string for the region as well.
global_aws_endpoint_with_empty_region_is_refused_test() ->
    ?assertThrow(
        ?ERR_BAD_VALUE_EMPTY(_, <<"region">>),
        storage_crud_utils:verify_s3_region(
            s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<>>})
        )
    ).


%% The helper takes an endpoint for the global one by its host, whatever the
%% scheme, the port and the path.
global_aws_endpoint_is_told_by_its_host_alone_test_() ->
    [
        ?_assertThrow(
            ?ERR_MISSING_REQUIRED_VALUE(_, <<"region">>),
            storage_crud_utils:verify_s3_region(s3_helper_spec(Scheme, Hostname, #{}))
        )
        || {Scheme, Hostname} <- [
            {<<"http">>, <<"s3.amazonaws.com:80">>},
            {<<"https">>, <<"s3.amazonaws.com:8443">>},
            {<<"https">>, <<"s3.amazonaws.com:443/">>}
        ]
    ].


global_aws_endpoint_with_region_passes_test() ->
    ?assertEqual(ok, storage_crud_utils:verify_s3_region(
        s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<"eu-west-3">>})
    )).


%% Neither a regional AWS endpoint nor a host that merely contains the global
%% one is the global endpoint, and neither is any other S3 service.
other_s3_endpoints_without_region_pass_test_() ->
    [
        ?_assertEqual(ok, storage_crud_utils:verify_s3_region(s3_helper_spec(<<"https">>, Hostname, #{})))
        || Hostname <- [
            <<"s3.eu-west-3.amazonaws.com:443">>,
            <<"some-bucket.s3.amazonaws.com:443">>,
            <<"s3.amazonaws.com.example.com:443">>,
            <<"minio.example.com:9000">>
        ]
    ].


%% A storage of another type has neither a hostname nor a region.
other_storage_types_pass_the_region_check_test() ->
    ?assertEqual(ok, storage_crud_utils:verify_s3_region(
        http_helper_spec(#{<<"credentialsType">> => <<"none">>})
    )).


%% A storage at the global endpoint may have no region, which its helper then
%% finds itself.
update_keeping_hostname_and_region_passes_without_region_test() ->
    ?assertEqual(ok, storage_crud_utils:verify_s3_region_update(
        s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{}),
        s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"fileMode">> => <<"0600">>})
    )).


update_to_global_aws_endpoint_without_region_is_refused_test() ->
    ?assertThrow(
        ?ERR_MISSING_REQUIRED_VALUE(_, <<"region">>),
        storage_crud_utils:verify_s3_region_update(
            s3_helper_spec(<<"https">>, <<"minio.example.com:9000">>, #{}),
            s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{})
        )
    ).


update_emptying_region_at_global_aws_endpoint_is_refused_test() ->
    ?assertThrow(
        ?ERR_BAD_VALUE_EMPTY(_, <<"region">>),
        storage_crud_utils:verify_s3_region_update(
            s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<"eu-west-3">>}),
            s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<>>})
        )
    ).


update_changing_region_at_global_aws_endpoint_passes_test() ->
    ?assertEqual(ok, storage_crud_utils:verify_s3_region_update(
        s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<"eu-west-3">>}),
        s3_helper_spec(<<"https">>, ?GLOBAL_AWS_ENDPOINT, #{<<"region">> => <<"us-west-2">>})
    )).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
s3_helper_spec(Scheme, Hostname, OtherConfiguration) ->
    #helper_spec{
        name = ?S3_HELPER_NAME,
        configuration = OtherConfiguration#{
            <<"scheme">> => Scheme,
            <<"hostname">> => Hostname,
            <<"bucketName">> => <<"some-bucket">>
        },
        credentials = #{<<"accessKey">> => <<"some-access-key">>, <<"secretKey">> => <<"some-secret-key">>}
    }.


%% @private
http_helper_spec(Credentials) ->
    #helper_spec{
        name = ?HTTP_HELPER_NAME,
        configuration = #{<<"endpoint">> => <<"https://example.com">>},
        credentials = Credentials
    }.


%% @private
pretty_print(Spec) ->
    lists:flatten(storage_crud_utils:pretty_print_spec(Spec)).


%% @private
external_luma_spec(ApiKey) ->
    #luma_spec{feed = external, url = <<"https://luma.example.com">>, api_key = ApiKey}.


%% @private
external_luma_spec_diff(ApiKey) ->
    #luma_spec_diff{feed = external, url = <<"https://luma.example.com">>, api_key = ApiKey}.
