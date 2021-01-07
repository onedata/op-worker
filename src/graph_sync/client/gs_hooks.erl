%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles running procedures in response to changes of the
%%% Oneprovider state concerning the Graph Sync interface with Onezone. Refer to
%%% the "Callback implementations" section to make changes in the procedures.
%%% The callbacks should throw on any error.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_hooks).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([handle_connected_to_oz/0]).
-export([handle_successful_healthcheck/0]).
-export([handle_disconnected_from_oz/0]).
-export([handle_deregistered_from_oz/0]).
-export([handle_entity_deleted/1]).

-define(PROVIDER_SYNC_PROGRESS_REPORT_INTERVAL,
    application:get_env(?APP_NAME, provider_sync_progress_report_interval_sec, 15)
).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Called when the Onezone connection is established.
%% @end
%%--------------------------------------------------------------------
-spec handle_connected_to_oz() -> ok | error.
handle_connected_to_oz() ->
    try
        ?info("Executing on-connect-to-oz procedures..."),
        on_connect_to_oz(),
        ?info("Finished executing on-connect-to-oz procedures")
    catch
        _:{_, ?ERROR_NO_CONNECTION_TO_ONEZONE} ->
            ?warning("Connection lost while running on-connect-to-oz procedures"),
            error;
        Class:Reason ->
            ?error_stacktrace("Failed to execute on-connect-to-oz procedures, disconnecting - ~w:~p", [
                Class, Reason
            ]),
            error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Called each time the Onezone connection is verified as healthy (every ?GS_RECONNECT_BASE_INTERVAL).
%% Errors are logged, but ignored.
%% @end
%%--------------------------------------------------------------------
-spec handle_successful_healthcheck() -> ok | error.
handle_successful_healthcheck() ->
    try
        on_successful_healthcheck()
    catch
        Class:Reason ->
            ?error_stacktrace("Failed to execute on-successful-healthcheck procedures - ~w:~p", [
                Class, Reason
            ])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Called when the Onezone connection is terminated to allow cleaning up.
%% Errors are logged, but ignored.
%% @end
%%--------------------------------------------------------------------
-spec handle_disconnected_from_oz() -> ok.
handle_disconnected_from_oz() ->
    try
        on_disconnect_from_oz()
    catch Class:Reason ->
        ?error_stacktrace("Failed to execute on-disconnect-from-oz procedures - ~w:~p", [
            Class, Reason
        ])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Called when the Oneprovider is deregistered from Onezone.
%% Errors are logged, but ignored.
%% @end
%%--------------------------------------------------------------------
-spec handle_deregistered_from_oz() -> ok.
handle_deregistered_from_oz() ->
    try
        ?notice("Provider has been deregistered - cleaning up credentials and config..."),
        on_deregister_from_oz(),
        ?notice("Oneprovider cleanup complete")
    catch Class:Reason ->
        ?error_stacktrace("Failed to execute on-deregister-from-oz procedures - ~w:~p", [
            Class, Reason
        ])
    end.


%%--------------------------------------------------------------------
%% @doc
%% Called when the Oneprovider received a push info that an entity has been deleted in Onezone.
%% Errors are logged, but ignored.
%% @end
%%--------------------------------------------------------------------
-spec handle_entity_deleted(gri:gri()) -> ok.
handle_entity_deleted(GRI) ->
    try
        on_entity_deleted(GRI),
        ok
    catch Class:Reason ->
        ?error_stacktrace("Failed to execute on-entity-deleted procedures for ~ts - ~w:~p", [
            gri:serialize(GRI), Class, Reason
        ])
    end.

%%%===================================================================
%%% Callback implementations
%%%===================================================================

%% @private
-spec on_connect_to_oz() -> ok | no_return().
on_connect_to_oz() ->
    ok = gs_client_worker:enable_cache(),
    ok = oneprovider:set_up_service_in_onezone(),
    ok = provider_logic:update_subdomain_delegation_ips(),
    ok = auth_cache:report_oz_connection_start(),
    ok = fslogic_worker:init_paths_caches(all),
    ok = main_harvesting_stream:revise_all_spaces(),
    ok = qos_bounded_cache:ensure_exists_for_all_spaces(),
    ok = rtransfer_config:add_storages(),
    ok = auto_storage_import_worker:notify_connection_to_oz(),
    ok = dbsync_worker:start_streams(),
    ok = qos_worker:init_retry_failed_files(),
    ok = provider_logic:revise_supported_spaces().


%% @private
-spec on_successful_healthcheck() -> ok | no_return().
on_successful_healthcheck() ->
    utils:throttle(?PROVIDER_SYNC_PROGRESS_REPORT_INTERVAL, fun report_provider_sync_progress/0).


%% @private
-spec on_disconnect_from_oz() -> ok | no_return().
on_disconnect_from_oz() ->
    ok = auth_cache:report_oz_connection_termination().


%% @private
-spec on_deregister_from_oz() -> ok.
on_deregister_from_oz() ->
    ok = provider_auth:delete(),
    ok = storage:clear_storages(),
    % kill the connection to prevent 'unauthorized' errors due
    % to older authorization when immediately registering anew
    ok = gs_client_worker:force_terminate().


%% @private
-spec on_entity_deleted(gri:gri()) -> ok | no_return().
on_entity_deleted(#gri{type = od_provider, id = ProviderId, aspect = instance}) ->
    case oneprovider:get_id_or_undefined() of
        ProviderId -> handle_deregistered_from_oz();
        _ -> ok
    end;
on_entity_deleted(#gri{type = od_space, id = SpaceId, aspect = instance}) ->
    ok = main_harvesting_stream:space_removed(SpaceId),
    ok = auto_storage_import_worker:notify_space_deleted(SpaceId);
on_entity_deleted(#gri{type = od_token, id = TokenId, aspect = instance}) ->
    ok = auth_cache:report_token_deletion(TokenId);
on_entity_deleted(#gri{type = temporary_token_secret, id = UserId, aspect = user}) ->
    ok = auth_cache:report_temporary_tokens_deletion(UserId);
on_entity_deleted(_) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec report_provider_sync_progress() -> ok.
report_provider_sync_progress() ->
    try
        {ok, Spaces} = provider_logic:get_spaces(),
        lists:foreach(fun report_provider_sync_progress/1, Spaces)
    catch Class:Reason ->
        ?error_stacktrace("Failed to report provider sync progress to Onezone due to ~w:~p", [Class, Reason])
    end.


%% @private
-spec report_provider_sync_progress(od_space:id()) -> ok.
report_provider_sync_progress(SpaceId) ->
    {ok, Providers} = space_logic:get_provider_ids(SpaceId),
    Report = provider_sync_progress:build_collective_report(Providers, fun(ProviderId) ->
        dbsync_state:get_seq_and_timestamp(SpaceId, ProviderId)
    end),
    space_logic:report_provider_sync_progress(SpaceId, Report).

