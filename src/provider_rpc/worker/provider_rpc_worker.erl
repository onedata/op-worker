%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module acts as limitless processes pool handling provider RPC requests.
%%% Requests concerning entities in spaces not supported by this provider are rejected).
%%% NOTE: All requests/results records are translated to protobuf.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_rpc_worker).
-author("Michał Stanisz").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([exec/2]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-type request() ::
    #provider_reg_distribution_get_request{} |
    #provider_current_dir_size_stats_browse_request{} |
    #provider_historical_dir_size_stats_browse_request{}.

-type result() ::
    #provider_reg_distribution_get_result{} |
    #provider_current_dir_size_stats_browse_result{} |
    #time_series_layout_get_result{} | #time_series_slice_get_result{}.

-export_type([request/0, result/0]).

-define(REQ(__FILE_GUID, __REQUEST),
    {provider_rpc_request, __FILE_GUID, __REQUEST}
).

-type internal_message() :: {provider_rpc_request, session:id(), file_id:file_guid(), request()}.

%%%===================================================================
%%% API
%%%===================================================================


-spec exec(file_id:file_guid(), request() | request()) ->
    ok | {ok, term()} | errors:error().
exec(FileGuid, Request) ->
    worker_proxy:call(?MODULE, ?REQ(FileGuid, Request)).


%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    {ok, #{}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck | internal_message()) ->
    pong | ok | {ok, term()} | errors:error().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle(?REQ(FileGuid, Request)) ->
    try
        middleware_utils:assert_file_managed_locally(FileGuid),
        FileCtx = file_ctx:new_by_guid(FileGuid),

        case provider_rpc_handlers:execute(FileCtx, Request) of
            {ok, Result} ->
                #provider_rpc_result{result = Result, status = ok};
            {error, _} = Error ->
                #provider_rpc_result{result = Error, status = error}
        end
    catch Type:Reason:Stacktrace ->
        error_utils:handle_error(Type, Reason, Stacktrace, ?ROOT_SESS_ID, Request)
    end;

handle(Request) ->
    ?log_bad_request(Request).


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok.
cleanup() ->
    % @TODO VFS-9402 move this to the node manager plugin callback before default workers stop
    gs_channel_service:terminate_internal_service(),
    ok.
