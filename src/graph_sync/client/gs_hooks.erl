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
-export([handle_healthcheck_success/0]).
-export([handle_disconnected_from_oz/0]).
-export([handle_deregistered_from_oz/0]).
-export([handle_entity_deleted/1]).

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
        Class:Reason:Stacktrace ->
            ?error_stacktrace("Failed to execute on-connect-to-oz procedures, disconnecting - ~w:~tp", [
                Class, Reason
            ], Stacktrace),
            error
    end.


-spec handle_healthcheck_success() -> ok.
handle_healthcheck_success() ->
    ok = oneprovider:ensure_service_set_up_in_onezone().


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
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace("Failed to run on-disconnect-from-oz procedures - ~w:~tp", [
            Class, Reason
        ], Stacktrace)
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
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace("Failed to run on-deregister-from-oz procedures - ~w:~tp", [
            Class, Reason
        ], Stacktrace)
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
    catch Class:Reason:Stacktrace ->
        ?error_stacktrace("Failed to run on-entity-deleted procedures for ~ts - ~w:~tp", [
            gri:serialize(GRI), Class, Reason
        ], Stacktrace)
    end.


%%%===================================================================
%%% Callback implementations
%%%===================================================================

%% NOTE: these procedures are run on a single cluster node.
%% @private
-spec on_connect_to_oz() -> ok | no_return().
on_connect_to_oz() ->
    ok = restart_hooks:maybe_execute_hooks(),
    ok = gs_client_worker:enable_cache(),
    ok = auth_cache:report_oz_connection_start(),
    ok = main_harvesting_stream:revise_all_spaces(),
    % TODO: VFS-5744 potential race condition:
    % provider may perform operations associated with QoS (or any other effective cache) before cache initialization
    ok = node_manager_plugin:init_etses_for_space_on_all_nodes(all),
    ok = rtransfer_config:add_storages(),
    ok = auto_storage_import_worker:notify_connection_to_oz(),
    ok = dbsync_worker:start_streams(),
    ok = qos_worker:init_retry_failed_files(),
    ok = qos_worker:init_traverse_pools(),
    ok = user_root_dir:ensure_cache_updated().


%% NOTE: these procedures are run on a single cluster node.
%% @private
-spec on_disconnect_from_oz() -> ok | no_return().
on_disconnect_from_oz() ->
    ok = auth_cache:report_oz_connection_termination().


%% NOTE: these procedures are run on a single cluster node.
%% @private
-spec on_deregister_from_oz() -> ok.
on_deregister_from_oz() ->
    ok = provider_auth:delete(),
    ok = storage:clear_storages(),
    % kill the connection to prevent 'unauthorized' errors due
    % to older authorization when immediately registering anew
    ok = gs_client_worker:force_terminate().


%% NOTE: these procedures are run on a single cluster node.
%% @private
-spec on_entity_deleted(gri:gri()) -> ok | no_return().
on_entity_deleted(#gri{type = od_provider, id = ProviderId, aspect = instance}) ->
    case oneprovider:get_id_or_undefined() of
        ProviderId -> handle_deregistered_from_oz();
        _ -> ok
    end;
on_entity_deleted(#gri{type = od_space, id = SpaceId, aspect = instance}) ->
    ok = od_space:handle_space_deleted(SpaceId);
on_entity_deleted(#gri{type = od_token, id = TokenId, aspect = instance}) ->
    ok = auth_cache:report_token_deletion(TokenId);
on_entity_deleted(#gri{type = temporary_token_secret, id = UserId, aspect = user}) ->
    ok = auth_cache:report_temporary_tokens_deletion(UserId);
on_entity_deleted(_) ->
    ok.
