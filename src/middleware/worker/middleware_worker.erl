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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_exec/3, exec/3]).

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).


-type operation() ::
    % atm related operations
    #schedule_atm_workflow_execution{} |
    #cancel_atm_workflow_execution{}.

-export_type([operation/0]).


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
    ok | {ok, term()} | no_return().
check_exec(SessionId, FileGuid, Operation) ->
    case exec(SessionId, FileGuid, Operation) of
        {error, _} = Error -> throw(Error);
        Result -> Result
    end.


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
        middleware_utils:assert_file_managed_locally(FileGuid),

        UserCtx = user_ctx:new(SessionId),
        FileCtx = file_ctx:new_by_guid(FileGuid),

        middleware_worker_request_router:route(UserCtx, FileCtx, Operation)
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
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
infer_error({error, Reason} = Error) ->
    case ordsets:is_element(Reason, ?ERROR_CODES) of
        true -> ?ERROR_POSIX(Reason);
        false -> Error
    end;

infer_error({badmatch, Error}) ->
    infer_error(Error);

infer_error(_Reason) ->
    %% TODO VFS-8614 replace unexpected error with internal server error
    ?ERROR_UNEXPECTED_ERROR(str_utils:rand_hex(5)).
