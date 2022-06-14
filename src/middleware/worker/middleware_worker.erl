%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module acts as limitless processes pool handling requests locally -
%%% no rerouting to other provider is made (requests concerning entities
%%% in spaces not supported by this provider are rejected).
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_worker).
-author("Bartosz Walkowicz").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("middleware/middleware.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([check_exec/3, exec/3]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

-type archive_operation() ::
    #list_archives{} |
    #archive_dataset{} |
    #get_archive_info{} |
    #update_archive{} |
    #delete_archive{} |
    #recall_archive{} |
    #cancel_archive_recall{} | 
    #get_recall_details{} |
    #get_recall_progress{} |
    #browse_recall_log{}.

-type atm_operation() ::
    #schedule_atm_workflow_execution{} |
    #cancel_atm_workflow_execution{} |
    #repeat_atm_workflow_execution{}.

-type dataset_operation() ::
    #list_top_datasets{} |
    #list_children_datasets{} |
    #establish_dataset{} |
    #get_dataset_info{} |
    #update_dataset{} |
    #remove_dataset {} |
    #get_file_eff_dataset_summary{}.

-type file_metadata_operations() ::
    #file_distribution_gather_request{} |
    #historical_dir_size_stats_gather_request{} |
    #file_storage_locations_get_request{}.

-type qos_operation() ::
    #add_qos_entry{} |
    #get_qos_entry{} |
    #remove_qos_entry{} |
    #get_effective_file_qos{} |
    #check_qos_status{}.

-type share_operation() ::
    #create_share{} |
    #remove_share{}.

-type transfer_operation() ::
    #schedule_file_transfer{} |
    #schedule_view_transfer{}.

-type operation() ::
    archive_operation() |
    atm_operation() |
    dataset_operation() |
    file_metadata_operations() |
    qos_operation() |
    share_operation() |
    transfer_operation().

-export_type([
    archive_operation/0, atm_operation/0, dataset_operation/0,
    file_metadata_operations/0, qos_operation/0, transfer_operation/0,
    operation/0
]).

-define(REQ(__SESSION_ID, __FILE_GUID, __OPERATION),
    {middleware_request, __SESSION_ID, __FILE_GUID, __OPERATION}
).


%%%===================================================================
%%% API
%%%===================================================================


-spec check_exec(session:id(), file_id:file_guid(), operation()) ->
    term() | no_return().
check_exec(SessionId, FileGuid, Operation) ->
    ?check(exec(SessionId, FileGuid, Operation)).


%% TODO VFS-8753 handle selector (e.g. {file, <FileGuid>}, {space, <SpaceId>}, etc.) as 2nd argument
-spec exec(session:id(), file_id:file_guid(), operation()) ->
    ok | {ok, term()} | errors:error().
exec(SessionId, FileGuid, Operation) ->
    worker_proxy:call(?MODULE, ?REQ(SessionId, FileGuid, Operation)).


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
-spec handle(ping | healthcheck | monitor_streams) ->
    pong | ok | {ok, term()} | errors:error().
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle(?REQ(SessionId, FileGuid, Operation)) ->
    try
        UserCtx = user_ctx:new(SessionId),
        assert_user_not_in_open_handle_mode(UserCtx),

        middleware_utils:assert_file_managed_locally(FileGuid),
        assert_file_access_not_in_share_mode(FileGuid),
        FileCtx = file_ctx:new_by_guid(FileGuid),

        middleware_worker_handlers:execute(UserCtx, FileCtx, Operation)
    catch Type:Reason:Stacktrace ->
        request_error_handler:handle(Type, Reason, Stacktrace, SessionId, Operation)
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_user_not_in_open_handle_mode(user_ctx:ctx()) -> ok | no_return().
assert_user_not_in_open_handle_mode(UserCtx) ->
    case user_ctx:is_in_open_handle_mode(UserCtx) of
        true -> throw(?ERROR_POSIX(?EPERM));
        false -> ok
    end.


%% @private
-spec assert_file_access_not_in_share_mode(file_id:file_guid()) -> ok | no_return().
assert_file_access_not_in_share_mode(FileGuid) ->
    case file_id:is_share_guid(FileGuid) of
        true -> throw(?ERROR_POSIX(?EPERM));
        false -> ok
    end.
