%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing an activity log for an OpenFaaS function - status reports
%%% (i.e. kubernetes events) collected for all pods that execute given function.
%%% The reports are kept in a form of an infinite log, one per each pod.
%%%
%%% NOTE: original timestamps of pod events are saved into a pod event log
%%% alongside the event payload. They are returned during listing instead of
%%% the internal timestamps generated by infinite log during appending.
%%% Thanks to this approach, the listing shows real timestamps of the events,
%%% but as a downside it is possible that the listed logs will not be strictly
%%% sorted (they may be some deviations if the logs were not submitted in order).
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_function_activity_registry).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([ensure_for_function/1]).
-export([delete/1]).
-export([get/1]).
-export([consume_report/1]).
-export([list_pod_events/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_openfaas_function_activity_registry{}.
-type doc() :: datastore_doc:doc(record()).

% identifier of a pod that executes an OpenFaaS function - each function can have
% multiple pods, which may be started or terminated at different moments.
-type pod_id() :: binary().

% status of a pod, as reported by Kubernetes (e.g. <<"Scheduled">> or <<"Created">>)
-type pod_status() :: binary().

% a single entry in a pod's infinite log of events (status reports)
-type pod_event() :: json_utils:json_map().

-export_type([id/0, diff/0, record/0, doc/0]).
-export_type([pod_id/0, pod_status/0, pod_event/0]).

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
        value = #atm_openfaas_function_activity_registry{
            pod_status_registry = atm_openfaas_function_pod_status_registry:empty()
        }
    },
    case datastore_model:create(?CTX, Doc) of
        {ok, _} ->
            {ok, RegistryId};
        {error, already_exists} ->
            {ok, RegistryId};
        {error, _} = Error ->
            Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(RegistryId) ->
    {ok, #document{value = #atm_openfaas_function_activity_registry{
        pod_status_registry = PodStatusRegistry
    }}} = datastore_model:get(?CTX, RegistryId),

    atm_openfaas_function_pod_status_registry:foreach_summary(fun(_PodId, PodStatusSummary) ->
        json_based_infinite_log_backend:destroy(
            PodStatusSummary#atm_openfaas_function_pod_status_summary.event_log
        )
    end, PodStatusRegistry),

    datastore_model:delete(?CTX, RegistryId).


-spec get(id()) -> {ok, record()} | {error, term()}.
get(RegistryId) ->
    case datastore_model:get(?CTX, RegistryId) of
        {ok, #document{value = Registry}} ->
            {ok, Registry};
        {error, _} = Error ->
            Error
    end.


-spec consume_report(atm_openfaas_function_activity_report:record()) -> ok.
consume_report(#atm_openfaas_function_activity_report{
    type = atm_openfaas_function_pod_status_report,
    batch = Batch
}) ->
    lists:foreach(fun consume_pod_status_report/1, Batch).


-spec list_pod_events(infinite_log:log_id(), infinite_log_browser:listing_opts()) ->
    {ok, {done | more, [{infinite_log:entry_index(), infinite_log:timestamp(), pod_event()}]}} | {error, term()}.
list_pod_events(LogId, ListingOpts) ->
    case json_based_infinite_log_backend:list(LogId, ListingOpts) of
        {error, _} = Error ->
            Error;
        {ok, {Marker, EntrySeries}} ->
            {ok, {Marker, lists:map(fun({EntryIndex, _InfiniteLogTimestamp, DecodedLogValue}) ->
                {EventTimestamp, EventPayload} = unpack_pod_event(DecodedLogValue),
                {EntryIndex, EventTimestamp, EventPayload}
            end, EntrySeries)}}
    end.

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
        {registry, {custom, string, {persistent_record, encode, decode, atm_openfaas_function_pod_status_registry}}}
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
-spec consume_pod_status_report(atm_openfaas_function_pod_status_report:record()) -> ok.
consume_pod_status_report(#atm_openfaas_function_pod_status_report{
    timestamp = Timestamp,
    function_name = FunctionName,
    pod_id = PodId,
    current_pod_status = CurrentPodStatus,
    pod_event = PodEvent
}) ->
    ActivityRegistryId = gen_registry_id(FunctionName),
    PodEventLogId = gen_pod_event_log_id(ActivityRegistryId, PodId),
    LogValue = pack_pod_event(Timestamp, PodEvent),

    ok = ?extract_ok(datastore_model:update(?CTX, ActivityRegistryId, fun(ActivityRegistry) ->
        {ok, ActivityRegistry#atm_openfaas_function_activity_registry{
            pod_status_registry = atm_openfaas_function_pod_status_registry:update_summary(
                PodId,
                fun(#atm_openfaas_function_pod_status_summary{
                    current_status_observation_timestamp = PreviousStatusObservationTimestamp
                } = PreviousSummary) ->
                    case Timestamp > PreviousStatusObservationTimestamp of
                        true ->
                            PreviousSummary#atm_openfaas_function_pod_status_summary{
                                current_status = CurrentPodStatus,
                                current_status_observation_timestamp = Timestamp
                            };
                        false ->
                            PreviousSummary
                    end
                end,
                #atm_openfaas_function_pod_status_summary{
                    current_status = CurrentPodStatus,
                    current_status_observation_timestamp = Timestamp,
                    event_log = PodEventLogId
                },
                ActivityRegistry#atm_openfaas_function_activity_registry.pod_status_registry
            )
        }}
    end)),

    case json_based_infinite_log_backend:append(PodEventLogId, LogValue) of
        ok ->
            ok;
        {error, not_found} ->
            ensure_pod_event_log(PodEventLogId),
            ok = json_based_infinite_log_backend:append(PodEventLogId, LogValue)
    end.


%% @private
-spec ensure_pod_event_log(infinite_log:log_id()) -> ok.
ensure_pod_event_log(LogId) ->
    case json_based_infinite_log_backend:create(LogId, #{}) of
        ok -> ok;
        {error, already_exists} -> ok
    end.


%% @private
-spec pack_pod_event(infinite_log:timestamp(), pod_event()) -> json_utils:json_map().
pack_pod_event(Timestamp, PodEvent) ->
    #{
        <<"timestamp">> => Timestamp,
        <<"payload">> => PodEvent
    }.


%% @private
-spec unpack_pod_event(json_utils:json_map()) -> {infinite_log:timestamp(), pod_event()}.
unpack_pod_event(#{
    <<"timestamp">> := Timestamp,
    <<"payload">> := PodEvent
}) ->
    {Timestamp, PodEvent}.