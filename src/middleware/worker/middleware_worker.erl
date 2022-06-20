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
-include("proto/oneprovider/mi_interprovider_messages.hrl").
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
    #dir_time_size_stats_gather_request{}.

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

% NOTE: translated to protobuf
-type interprovider_operation() ::
    #local_reg_file_distribution_get_request{} |
    #browse_local_current_dir_size_stats{} |
    #browse_local_time_dir_size_stats{}.

% NOTE: translated to protobuf
-type interprovider_result() ::
    #local_current_dir_size_stats{} |
    #provider_reg_distribution{} |
    #time_series_layout_result{} |
    #time_series_slice_result{}.

-export_type([interprovider_operation/0, interprovider_result/0]).


-define(SHOULD_LOG_REQUESTS_ON_ERROR, application:get_env(
    ?CLUSTER_WORKER_APP_NAME, log_requests_on_error, false
)).

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
-spec exec(session:id(), file_id:file_guid(), operation() | interprovider_operation()) ->
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
        handle_error(Type, Reason, Stacktrace, SessionId, Operation)
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


%% @private
-spec handle_error(
    Type :: atom(),
    Reason :: term(),
    Stacktrace :: list(),
    session:id(),
    operation()
) ->
    errors:error().
handle_error(throw, Reason, _Stacktrace, _SessionId, _Request) ->
    infer_error(Reason);

handle_error(_Type, Reason, Stacktrace, SessionId, Request) ->
    Error = infer_error(Reason),

    {LogFormat, LogFormatArgs} = case ?SHOULD_LOG_REQUESTS_ON_ERROR of
        true ->
            MF = "Cannot process request ~p for session ~p due to: ~p caused by ~p",
            FA = [lager:pr(Request, ?MODULE), SessionId, Error, Reason],
            {MF, FA};
        false ->
            MF = "Cannot process request for session ~p due to: ~p caused by ~p",
            FA = [SessionId, Error, Reason],
            {MF, FA}
    end,

    case Error of
        ?ERROR_UNEXPECTED_ERROR(_) ->
            ?error_stacktrace(LogFormat, LogFormatArgs, Stacktrace);
        _ ->
            ?debug_stacktrace(LogFormat, LogFormatArgs, Stacktrace)
    end,

    Error.


%% @private
-spec infer_error(term()) -> errors:error().
infer_error({badmatch, Error}) ->
    infer_error(Error);

infer_error({error, Reason} = Error) ->
    case ordsets:is_element(Reason, ?ERROR_CODES) of
        true -> ?ERROR_POSIX(Reason);
        false -> Error
    end;

infer_error(Reason) ->
    case ordsets:is_element(Reason, ?ERROR_CODES) of
        true ->
            ?ERROR_POSIX(Reason);
        false ->
            %% TODO VFS-8614 replace unexpected error with internal server error
            ?ERROR_UNEXPECTED_ERROR(str_utils:rand_hex(5))
    end.
