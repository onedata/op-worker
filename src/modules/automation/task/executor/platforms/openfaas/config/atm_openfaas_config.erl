%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides interface to inspect OpenFaaS service configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_config).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    get/0,

    get_endpoint/2,
    get_basic_auth_header/1,
    get_function_namespace/1,
    get_activity_feed_secret/1,
    get_oneclient_image/1,
    get_oneclient_options/1,
    get_result_streamer_image/1
]).

-record(atm_openfaas_config, {
    url :: binary(),
    basic_auth :: binary(),
    function_namespace :: binary(),
    activity_feed_secret :: binary()
}).
-opaque record() :: #atm_openfaas_config{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get() -> record() | no_return().
get() ->
    Host = get_env(openfaas_host),
    Port = get_env(openfaas_port),

    AdminUsername = get_env(openfaas_admin_username),
    AdminPassword = get_env(openfaas_admin_password),
    Hash = base64:encode(str_utils:format_bin("~ts:~ts", [AdminUsername, AdminPassword])),

    #atm_openfaas_config{
        url = str_utils:format_bin("http://~ts:~B", [Host, Port]),
        basic_auth = <<"Basic ", Hash/binary>>,
        function_namespace = str_utils:to_binary(get_env(openfaas_function_namespace)),
        activity_feed_secret = str_utils:to_binary(get_env(openfaas_activity_feed_secret))
    }.


-spec get_endpoint(record(), binary()) -> binary().
get_endpoint(#atm_openfaas_config{url = OpenfaasUrl}, Path) ->
    str_utils:format_bin("~ts~ts", [OpenfaasUrl, Path]).


-spec get_basic_auth_header(record()) -> map().
get_basic_auth_header(#atm_openfaas_config{basic_auth = BasicAuth}) ->
    #{?HDR_AUTHORIZATION => BasicAuth}.


-spec get_function_namespace(record()) -> binary().
get_function_namespace(#atm_openfaas_config{function_namespace = FunctionNamespace}) ->
    FunctionNamespace.


-spec get_activity_feed_secret(record()) -> binary().
get_activity_feed_secret(#atm_openfaas_config{activity_feed_secret = ActivityFeedSecret}) ->
    ActivityFeedSecret.


-spec get_oneclient_image(record()) -> binary().
get_oneclient_image(_Record) ->
    case get_env(openfaas_oneclient_image, undefined) of
        undefined ->
            ReleaseVersion = op_worker:get_release_version(),
            <<"onedata/oneclient:", ReleaseVersion/binary>>;
        OneclientImage ->
            str_utils:to_binary(OneclientImage)
    end.


-spec get_oneclient_options(record()) -> binary().
get_oneclient_options(_Record) ->
    str_utils:to_binary(get_env(openfaas_oneclient_options, <<"">>)).


-spec get_result_streamer_image(record()) -> binary().
get_result_streamer_image(_Record) ->
    case get_env(openfaas_result_streamer_image, undefined) of
        undefined ->
            ReleaseVersion = op_worker:get_release_version(),
            <<"onedata/openfaas-lambda-result-streamer:", ReleaseVersion/binary>>;
        ResultStreamerImage ->
            str_utils:to_binary(ResultStreamerImage)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_env(atom()) -> term() | no_return().
get_env(Key) ->
    case get_env(Key, undefined) of
        undefined -> throw(?ERROR_ATM_OPENFAAS_NOT_CONFIGURED);
        Value -> Value
    end.


%% @private
-spec get_env(atom(), term()) -> term().
get_env(Key, Default) ->
    op_worker:get_env(Key, Default).
