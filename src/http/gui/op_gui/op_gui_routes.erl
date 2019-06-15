%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles:
%%%   - what modules handles server logic of WebSocket connection with
%%%         the client (data backends)
%%%   - sending session details to the client
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_routes).
-author("Lukasz Opiola").


-include("global_definitions.hrl").

-export([data_backend/2]).
-export([session_details/0]).

%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback data_backend/2
%% @end
%%--------------------------------------------------------------------
-spec data_backend(HasSession :: boolean(), Identifier :: binary()) ->
    HandlerModule :: module().
data_backend(true, <<"user">>) -> user_data_backend;

data_backend(true, <<"space">>) -> space_data_backend;
data_backend(true, <<"space-user-list">>) -> space_data_backend;
data_backend(true, <<"space-group-list">>) -> space_data_backend;
data_backend(true, <<"space-provider-list">>) -> space_data_backend;
data_backend(true, <<"space-transfer-list">>) -> space_data_backend;
data_backend(true, <<"space-transfer-stat">>) -> space_data_backend;
data_backend(true, <<"space-transfer-time-stat">>) -> space_data_backend;
data_backend(true, <<"space-transfer-link-state">>) -> space_data_backend;
data_backend(true, <<"space-on-the-fly-transfer-list">>) -> space_data_backend;

data_backend(true, <<"share">>) -> share_data_backend;
data_backend(_, <<"share-public">>) -> share_data_backend;

data_backend(true, <<"handle-service">>) -> handle_service_data_backend;

data_backend(true, <<"handle">>) -> handle_data_backend;
data_backend(_, <<"handle-public">>) -> handle_data_backend;

data_backend(true, <<"system-provider">>) -> system_data_backend;
data_backend(true, <<"system-user">>) -> system_data_backend;
data_backend(true, <<"system-group">>) -> system_data_backend;

data_backend(true, <<"file">>) -> file_data_backend;
data_backend(true, <<"file-shared">>) -> file_data_backend;
data_backend(_, <<"file-public">>) -> file_data_backend;

data_backend(true, <<"file-permission">>) -> file_permissions_data_backend;

data_backend(true, <<"file-distribution">>) -> file_distribution_data_backend;

data_backend(true, <<"file-property">>) -> metadata_data_backend;
data_backend(_, <<"file-property-public">>) -> metadata_data_backend;
data_backend(true, <<"file-property-shared">>) -> metadata_data_backend;

data_backend(true, <<"transfer">>) -> transfer_data_backend;
data_backend(true, <<"on-the-fly-transfer">>) -> transfer_data_backend;
data_backend(true, <<"transfer-time-stat">>) -> transfer_data_backend;
data_backend(true, <<"transfer-current-stat">>) -> transfer_data_backend;

data_backend(true, <<"db-index">>) -> db_index_data_backend.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback get_session_details/0
%% @end
%%--------------------------------------------------------------------
-spec session_details() ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
session_details() ->
    ProviderId = oneprovider:get_id(),
    {ok, ProviderName} = provider_logic:get_name(ProviderId),
    TransfersHistoryLimitPerFile = application:get_env(
        ?APP_NAME, transfers_history_limit_per_file, 100
    ),
    Res = [
        {<<"userId">>, op_gui_session:get_user_id()},
        {<<"providerId">>, ProviderId},
        {<<"providerName">>, ProviderName},
        {<<"onezoneURL">>,
            oneprovider:get_oz_url()
        },
        {<<"manageProvidersURL">>,
            str_utils:to_binary(oneprovider:get_oz_providers_page())
        },
        {<<"serviceVersion">>, oneprovider:get_version()},
        {<<"config">>, [
            {<<"transfersHistoryLimitPerFile">>, TransfersHistoryLimitPerFile}
        ]}
    ],
    {ok, Res}.
