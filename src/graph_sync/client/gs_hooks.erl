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
-export([handle_disconnected_from_oz/0]).
-export([handle_deregistered_from_oz/0]).

-define(HOOK_TIMEOUT, 120000).  % 2 minutes

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
        ?info("Successfully executed on-connect-to-oz procedures")
    catch
        _:{_, ?ERROR_NO_CONNECTION_TO_ONEZONE} ->
            ?warning("Connection lost while running on-connect-to-oz procedures");
        Class:Reason ->
            ?error_stacktrace("Failed to execute on-connect-to-oz procedures, disconnecting - ~w:~p", [
                Class, Reason
            ]),
            error
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
        ?error_stacktrace("Failed to run on-disconnect-from-oz procedures - ~w:~p", [
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
        ?error_stacktrace("Failed to run on-deregister-from-oz procedures - ~w:~p", [
            Class, Reason
        ])
    end.

%%%===================================================================
%%% Callback implementations
%%%===================================================================

%% @private
-spec on_connect_to_oz() -> ok | no_return().
on_connect_to_oz() ->
    ok = oneprovider:set_up_service_in_onezone(),
    ok = provider_logic:update_subdomain_delegation_ips(),
    ok = auth_cache:report_oz_connection_start(),
    ok = fslogic_worker:init_paths_caches(all),
    ok = main_harvesting_stream:revise_all_spaces(),
    ok = qos_bounded_cache:ensure_exists_for_all_spaces(),
    ok = rtransfer_config:add_storages(),
    ok = storage_sync_worker:notify_connection_to_oz(),
    ok = dbsync_worker:start_streams(),
    ok = qos_worker:init_retry_failed_files().


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
