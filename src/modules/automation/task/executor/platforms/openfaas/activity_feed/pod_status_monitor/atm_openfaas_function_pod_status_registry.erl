%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about pods that execute a specific OpenFaaS function;
%%% the current status of each pod and the history of all status change reports
%%% (based on kubernetes events). The reports are kept in a form of an infinite log,
%%% one per each pod.
%%%
%%% NOTE: original timestamps of pod events are saved into a pod event log
%%% alongside the event payload. They are returned during listing instead of
%%% the internal timestamps generated by infinite log during appending.
%%% Thanks to this approach, the listing shows real timestamps of the events,
%%% but as a downside it is possible that the listed logs will not be strictly
%%% sorted (they may be some deviations if the logs were not submitted in order).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_function_pod_status_registry).
-author("Lukasz Opiola").

-behaviour(atm_openfaas_activity_report_handler).

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([ensure_for_function/1, get/1, delete/1]).
-export([to_json/1]).
-export([find_summary/2, foreach_summary/2]).
-export([browse_pod_event_log/2]).

%% atm_openfaas_activity_report_handler callbacks
-export([consume_report/3]).
-export([handle_error/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_openfaas_function_pod_status_registry{}.
-type doc() :: datastore_doc:doc(record()).

% identifier of a pod that executes an OpenFaaS function - each function can have
% multiple pods, which may be started or terminated at different moments
-type pod_id() :: binary().

% Jsonable object containing all information related to an event, stored in the
% database in the same format as later returned during listing. Does not include
% the log index, which is added after entries are listed.
-type event_data() :: json_utils:json_map().

-export_type([id/0, diff/0, record/0, doc/0]).
-export_type([pod_id/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec ensure_for_function(atm_openfaas_task_executor:function_name()) ->
    {ok, id()} | {error, term()}.
ensure_for_function(FunctionName) ->
    RegistryId = gen_registry_id(FunctionName),
    Doc = #document{
        key = RegistryId,
        value = #atm_openfaas_function_pod_status_registry{}
    },
    case datastore_model:create(?CTX, Doc) of
        {ok, _} ->
            {ok, RegistryId};
        {error, already_exists} ->
            {ok, RegistryId};
        {error, _} = Error ->
            Error
    end.


-spec get(id()) -> {ok, record()} | {error, term()}.
get(RegistryId) ->
    case datastore_model:get(?CTX, RegistryId) of
        {ok, #document{value = Registry}} ->
            {ok, Registry};
        {error, _} = Error ->
            Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(RegistryId) ->
    {ok, PodStatusRegistry} = ?MODULE:get(RegistryId),

    foreach_summary(fun(_PodId, PodStatusSummary) ->
        json_infinite_log_model:destroy(
            PodStatusSummary#atm_openfaas_function_pod_status_summary.event_log
        )
    end, PodStatusRegistry),

    datastore_model:delete(?CTX, RegistryId).


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    maps:map(fun(_PodId, PodStatusSummary) ->
        jsonable_record:to_json(PodStatusSummary, atm_openfaas_function_pod_status_summary)
    end, Registry).


-spec find_summary(atm_openfaas_function_pod_status_registry:pod_id(), record()) ->
    {ok, atm_openfaas_function_pod_status_summary:record()} | error.
find_summary(PodId, #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    maps:find(PodId, Registry).


-spec foreach_summary(
    fun((atm_openfaas_function_pod_status_registry:pod_id(), atm_openfaas_function_pod_status_summary:record()) -> term()),
    record()
) -> ok.
foreach_summary(Callback, #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    maps:foreach(Callback, Registry).


-spec browse_pod_event_log(infinite_log:log_id(), json_infinite_log_model:listing_opts()) ->
    {ok, json_infinite_log_model:browse_result()} | {error, term()}.
browse_pod_event_log(LogId, ListingOpts) ->
    case json_infinite_log_model:browse_content(LogId, ListingOpts) of
        {error, _} = Error ->
            Error;
        {ok, Data} ->
            {ok, maps:update_with(<<"logEntries">>, fun(LogEntries) ->
                lists:map(fun(#{
                    <<"content">> := #{
                        <<"timestamp">> := EventTimestamp
                    } = Content
                } = Entry) ->
                    Entry#{
                        <<"timestamp">> => EventTimestamp,
                        <<"content">> => maps:remove(<<"timestamp">>, Content)
                    }
                end, LogEntries)
            end, Data)}
    end.

%%%===================================================================
%%% atm_openfaas_activity_report_handler callbacks
%%%===================================================================

-spec consume_report(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_activity_report:body(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    {no_reply | {reply_json, json_utils:json_term()}, atm_openfaas_activity_feed_ws_handler:handler_state()}.
consume_report(_ConnRef, PodStatusReport, HandlerState) ->
    consume_pod_status_report(PodStatusReport),
    {no_reply, HandlerState}.


-spec handle_error(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    errors:error(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    ok.
handle_error(_ConnRef, _Error, _HandlerState) ->
    ok.

%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {registry, #{
            string => {custom, string, {persistent_record, encode, decode, atm_openfaas_function_pod_status_summary}}
        }}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec gen_registry_id(atm_openfaas_task_executor:function_name()) -> id().
gen_registry_id(FunctionName) ->
    datastore_key:new_from_digest([FunctionName]).


%% @private
-spec gen_pod_event_log_id(id(), pod_id()) -> infinite_log:log_id().
gen_pod_event_log_id(RegistryId, PodId) ->
    datastore_key:adjacent_from_digest([PodId], RegistryId).


%% @private
-spec ensure_pod_event_log(infinite_log:log_id()) -> ok.
ensure_pod_event_log(LogId) ->
    case json_infinite_log_model:create(LogId, #{}) of
        ok -> ok;
        {error, already_exists} -> ok
    end.


%% @private
-spec consume_pod_status_report(atm_openfaas_function_pod_status_report:record()) -> ok.
consume_pod_status_report(#atm_openfaas_function_pod_status_report{
    function_name = FunctionName,
    pod_id = PodId,

    event_timestamp = EventTimestamp,
    event_type = EventType,
    event_reason = EventReason,
    event_message = EventMessage
} = PodStatusReport) ->
    PodStatusRegistryId = gen_registry_id(FunctionName),

    {ok, _} = datastore_model:update(?CTX, PodStatusRegistryId, fun(PodStatusRegistry) ->
        {ok, apply_report_to_corresponding_summary(PodStatusReport, PodStatusRegistry)}
    end),

    PodEventLogId = gen_pod_event_log_id(PodStatusRegistryId, PodId),
    EventData = build_event_data(EventTimestamp, EventType, EventReason, EventMessage),
    case json_infinite_log_model:append(PodEventLogId, EventData) of
        ok ->
            ok;
        {error, not_found} ->
            ensure_pod_event_log(PodEventLogId),
            ok = json_infinite_log_model:append(PodEventLogId, EventData)
    end.


%% @private
-spec apply_report_to_corresponding_summary(atm_openfaas_function_pod_status_report:record(), record()) ->
    record().
apply_report_to_corresponding_summary(#atm_openfaas_function_pod_status_report{
    function_name = FunctionName,
    pod_id = PodId,

    pod_status = NewPodStatus,
    containers_readiness = ContainersReadiness,

    event_timestamp = EventTimestamp
}, PodStatusRegistry) ->
    PodStatusId = gen_registry_id(FunctionName),
    PodEventLogId = gen_pod_event_log_id(PodStatusId, PodId),
    Default = #atm_openfaas_function_pod_status_summary{
        current_status = NewPodStatus,
        current_containers_readiness = ContainersReadiness,
        last_status_change_timestamp = EventTimestamp,
        event_log = PodEventLogId
    },
    update_summary(PodId, fun(#atm_openfaas_function_pod_status_summary{
        current_status = PreviousPodStatus,
        last_status_change_timestamp = PreviousStatusChangeTimestamp
    } = PreviousSummary) ->
        case EventTimestamp >= PreviousStatusChangeTimestamp of
            true ->
                PreviousSummary#atm_openfaas_function_pod_status_summary{
                    current_status = NewPodStatus,
                    current_containers_readiness = ContainersReadiness,
                    last_status_change_timestamp = case NewPodStatus of
                        PreviousPodStatus -> PreviousStatusChangeTimestamp;
                        _ -> EventTimestamp
                    end
                };
            false ->
                PreviousSummary
        end
    end, Default, PodStatusRegistry).


%% @private
-spec update_summary(
    atm_openfaas_function_pod_status_registry:pod_id(),
    fun((atm_openfaas_function_pod_status_summary:record()) -> atm_openfaas_function_pod_status_summary:record()),
    atm_openfaas_function_pod_status_summary:record(),
    record()
) ->
    record().
update_summary(PodId, Diff, Default, Record = #atm_openfaas_function_pod_status_registry{registry = Registry}) ->
    Record#atm_openfaas_function_pod_status_registry{
        registry = maps:update_with(PodId, Diff, Default, Registry)
    }.


%% @private
-spec build_event_data(
    atm_openfaas_function_pod_status_report:event_timestamp(),
    atm_openfaas_function_pod_status_report:event_type(),
    atm_openfaas_function_pod_status_report:event_reason(),
    atm_openfaas_function_pod_status_report:event_message()
) -> event_data().
build_event_data(EventTimestamp, EventType, EventReason, EventMessage) ->
    #{
        <<"timestamp">> => EventTimestamp,
        <<"type">> => EventType,
        <<"reason">> => EventReason,
        <<"message">> => EventMessage
    }.
