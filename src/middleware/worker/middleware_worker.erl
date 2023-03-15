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
-include("modules/fslogic/data_distribution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([check_exec/3, exec/3]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

-type archive_operation() ::
    #archives_list_request{} |
    #dataset_archive_request{} |
    #archivisation_cancel_request{} |
    #archive_info_get_request{} |
    #archive_update_request{} |
    #archive_delete_request{} |
    #archive_recall_request{} |
    #archive_recall_cancel_request{} |
    #archive_recall_details_get_request{} |
    #archive_recall_progress_get_request{} |
    #archive_recall_log_browse_request{}.

-type atm_operation() ::
    #atm_workflow_execution_schedule_request{} |
    #atm_workflow_execution_init_cancel_request{} |
    #atm_workflow_execution_init_pause_request{} |
    #atm_workflow_execution_resume_request{} |
    #atm_workflow_execution_repeat_request{} |
    #atm_workflow_execution_discard_request{}.

-type cdmi_operation() ::
    #transfer_encoding_get_request{} |
    #transfer_encoding_set_request{} |
    #cdmi_completion_status_get_request{} |
    #cdmi_completion_status_set_request{} |
    #mimetype_get_request{} |
    #mimetype_set_request{}.

-type dataset_operation() ::
    #top_datasets_list_request{} |
    #children_datasets_list_request{} |
    #dataset_establish_request{} |
    #dataset_info_get_request{} |
    #dataset_update_request{} |
    #dataset_remove_request {} |
    #file_eff_dataset_summary_get_request{}.

-type file_metadata_operations() ::
    #custom_metadata_get_request{} |
    #custom_metadata_set_request{} |
    #custom_metadata_remove_request{} |
    #data_distribution_gather_request{} |
    #historical_dir_size_stats_get_request{} |
    #file_storage_locations_get_request{}.

-type qos_operation() ::
    #qos_entry_add_request{} |
    #qos_entry_get_request{} |
    #qos_entry_remove_request{} |
    #effective_file_qos_get_request{} |
    #qos_status_check_request{}.

-type share_operation() ::
    #share_create_request{} |
    #share_remove_request{}.

-type transfer_operation() ::
    #file_transfer_schedule_request{} |
    #view_transfer_schedule_request{}.

-type operation() ::
    archive_operation() |
    atm_operation() |
    cdmi_operation() |
    dataset_operation() |
    file_metadata_operations() |
    qos_operation() |
    share_operation() |
    transfer_operation().

-export_type([
    archive_operation/0, atm_operation/0,
    cdmi_operation/0, dataset_operation/0,
    file_metadata_operations/0, 
    qos_operation/0, transfer_operation/0,
    operation/0
]).

-define(OPERATIONS_AVAILABLE_IN_SHARE_MODE, [
    custom_metadata_get_request
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
        FileCtx = file_ctx:new_by_guid(FileGuid),
        UserCtx = infer_user_ctx(SessionId, FileCtx, Operation),

        middleware_utils:assert_file_managed_locally(FileGuid),

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
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec infer_user_ctx(session:id(), file_ctx:ctx(), operation()) ->
    user_ctx:ctx() | no_return().
infer_user_ctx(SessionId, FileCtx, Operation) ->
    UserCtx = user_ctx:new(SessionId),

    assert_user_not_in_open_handle_mode(UserCtx),
    ensure_guest_ctx_in_case_of_share_mode(UserCtx, FileCtx, Operation).


%% @private
-spec assert_user_not_in_open_handle_mode(user_ctx:ctx()) -> ok | no_return().
assert_user_not_in_open_handle_mode(UserCtx) ->
    case user_ctx:is_in_open_handle_mode(UserCtx) of
        true -> throw(?ERROR_POSIX(?EPERM));
        false -> ok
    end.


%% @private
-spec ensure_guest_ctx_in_case_of_share_mode(user_ctx:ctx(), file_ctx:ctx(), operation()) ->
    user_ctx:ctx() | no_return().
ensure_guest_ctx_in_case_of_share_mode(UserCtx, FileCtx, Operation) ->
    case file_ctx:get_share_id_const(FileCtx) of
        undefined ->
            UserCtx;
        _ShareId ->
            case is_operation_available_in_share_mode(Operation) of
                true -> ensure_guest_ctx(UserCtx);
                false -> throw(?ERROR_POSIX(?EPERM))
            end
    end.


%% @private
-spec is_operation_available_in_share_mode(operation()) -> boolean().
is_operation_available_in_share_mode(Operation) ->
    lists:member(get_operation_name(Operation), ?OPERATIONS_AVAILABLE_IN_SHARE_MODE).


%% @private
-spec get_operation_name(operation()) -> atom().
get_operation_name(Operation) ->
    element(1, Operation).


%% @private
-spec ensure_guest_ctx(user_ctx:ctx()) -> user_ctx:ctx().
ensure_guest_ctx(UserCtx) ->
    case user_ctx:is_guest(UserCtx) of
        true -> UserCtx;
        false -> user_ctx:new(?GUEST_SESS_ID)
    end.
