%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of DBSync worker. For each space it holds mapping from
%%% provider to a sequence number of the beginning of expected changes range.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_state).
-author("Krzysztof Trzepla").

-include("modules/dbsync/dbsync.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([delete/1, get_seq/2, get_seq_and_timestamp/2, set_seq_and_timestamp/4,
    resynchronize_stream/5, get_synchronization_params/2, maybe_set_initial_sync/3,
    set_initial_sync_repeat/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-define(CTX, #{model => ?MODULE}).

-type state() :: #dbsync_state{}.
-type synchronization_mode() :: resynchronization | initial_sync | initial_sync_to_repeat.
-type synchronization_params() :: #synchronization_params{}.
-type synchronization_params_map() ::#{od_provider:id() => synchronization_params()}.
-export_type([synchronization_mode/0, synchronization_params_map/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes dbsync state for a space.
%% @end
%%--------------------------------------------------------------------
-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns sequence number of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(od_space:id(), od_provider:id()) -> couchbase_changes:seq().
get_seq(SpaceId, ProviderId) ->
    {Seq, _Timestamp} = get_seq_and_timestamp(SpaceId, ProviderId),
    Seq.

%%--------------------------------------------------------------------
%% @doc
%% Returns sequence number and timestamp of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec get_seq_and_timestamp(od_space:id(), od_provider:id()) -> {couchbase_changes:seq(), datastore_doc:timestamp()}.
get_seq_and_timestamp(SpaceId, ProviderId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{seq = Seq}}} ->
            maps:get(ProviderId, Seq, {1, 0});
        {error, not_found} ->
            {1, 0}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets sequence number and timestamp of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec set_seq_and_timestamp(od_space:id(), od_provider:id(), couchbase_changes:seq(),
    dbsync_changes:timestamp() | undefined) -> continue | reset | {error, Reason :: term()}.
set_seq_and_timestamp(SpaceId, ProviderId, Number, Timestamp) ->
    {Diff, Default} = case Timestamp of
        undefined ->
            DiffFun = fun(#dbsync_state{seq = Seq} = State) ->
                % Use old timestamp (no doc with timestamp in applied range)
                {_, Timestamp2} = maps:get(ProviderId, Seq, {1, 0}),
                {ok, set_seq_and_timestamp_internal(State, ProviderId, Number, Timestamp2)}
            end,
            {DiffFun, #dbsync_state{seq = #{ProviderId => {Number, 0}}}};
        _ ->
            DiffFun = fun(State) ->
                {ok, set_seq_and_timestamp_internal(State, ProviderId, Number, Timestamp)}
            end,
            {DiffFun, #dbsync_state{seq = #{ProviderId => {Number, Timestamp}}}}
    end,

    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, #document{value = #dbsync_state{seq = SeqMap}}} ->
            case maps:get(ProviderId, SeqMap, undefined) of
                {Number, _} -> continue;
                _ -> reset
            end;
        {error, Reason} -> {error, Reason}
    end.


-spec resynchronize_stream(od_space:id(), od_provider:id(), dbsync_in_stream:mutators(),
    dbsync_in_stream:sync_start_seq(), dbsync_in_stream:sync_target_seq()) -> ok | {error, Reason :: term()}.
resynchronize_stream(SpaceId, ProviderId, IncludedMutators, StartSeq, TargetSeq) ->
    DiffFun = fun(#dbsync_state{seq = Seq, synchronization_params = Params} = State) ->
        {CurrentSeq, _} = Current = maps:get(ProviderId, Seq, {1, 0}),
        case CurrentSeq > 1 andalso StartSeq =/= 0 andalso StartSeq < CurrentSeq of
            true ->
                FinalStartSeq = case StartSeq > 0 of
                    true -> StartSeq;
                    false -> max(CurrentSeq + StartSeq, 1) % StartSeq is negative
                end,
                {FinalTargetSeq, SeqAndTimestampToRestore} = case TargetSeq of
                    PastSeq when is_integer(TargetSeq) andalso TargetSeq < CurrentSeq -> {PastSeq, Current};
                    _ -> {CurrentSeq, undefined}
                end,

                {ok, State#dbsync_state{
                    seq = Seq#{ProviderId => {FinalStartSeq, 0}},
                    synchronization_params = Params#{ProviderId => #synchronization_params{
                        mode = resynchronization,
                        target_seq = FinalTargetSeq,
                        included_mutators = IncludedMutators,
                        seq_with_timestamp_to_restore = SeqAndTimestampToRestore
                    }}
                }};
            false ->
                {error, nothing_to_resynchronize}
        end
    end,

    case datastore_model:update(?CTX, SpaceId, DiffFun) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, nothing_to_resynchronize} -> ok;
        {error, Reason} -> {error, Reason}
    end.


-spec get_synchronization_params(od_space:id(), od_provider:id()) -> synchronization_params() | undefined.
get_synchronization_params(SpaceId, ProviderId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{synchronization_params = Params}}} ->
            maps:get(ProviderId, Params, undefined);
        {error, not_found} ->
            undefined
    end.


-spec maybe_set_initial_sync(od_space:id(), od_provider:id(), couchbase_changes:seq()) -> 
    synchronization_params() | undefined.
maybe_set_initial_sync(SpaceId, ProviderId, TargetSeq) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{seq = Seq, synchronization_params = Params}}} ->
            ProviderParams = maps:get(ProviderId, Params, undefined),
            case {maps:get(ProviderId, Seq, {1, 0}), ProviderParams} of
                {{1, 0}, undefined} ->
                    maybe_set_initial_sync_internal(SpaceId, ProviderId, TargetSeq);
                _ ->
                    ProviderParams
            end;
        {error, not_found} ->
            maybe_set_initial_sync_internal(SpaceId, ProviderId, TargetSeq)
    end.


-spec set_initial_sync_repeat(od_space:id()) -> ok | {error, initial_sync_does_not_exist}.
set_initial_sync_repeat(SpaceId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{synchronization_params = ParamsToCheck}}} ->
            IsAnyInitialSync = is_any_synchronization_in_mode(initial_sync, ParamsToCheck),
            IsAnyInitialSyncToRepeat = is_any_synchronization_in_mode(initial_sync_to_repeat, ParamsToCheck),

            case {IsAnyInitialSync, IsAnyInitialSyncToRepeat} of
                {true, _} ->
                    DiffFun = fun(#dbsync_state{synchronization_params = Params} = State) ->
                        UpdatedParams = maps:fold(fun
                            (ProviderId, #synchronization_params{mode = initial_sync} = ProviderParams, Acc) ->
                                Acc#{ProviderId => ProviderParams#synchronization_params{mode = initial_sync_to_repeat}};
                            (_, _, Acc) ->
                                Acc
                        end, Params, Params),
                        {ok, State#dbsync_state{synchronization_params = UpdatedParams}}
                    end,

                    {ok, _} = datastore_model:update(?CTX, SpaceId, DiffFun),
                    ok;
                {false, true} ->
                    ok;
                {false, false} ->
                    {error, initial_sync_does_not_exist}
            end;
        {error, not_found} ->
            {error, initial_sync_does_not_exist}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_set_initial_sync_internal(od_space:id(), od_provider:id(), couchbase_changes:seq()) ->
    synchronization_params() | undefined.
maybe_set_initial_sync_internal(SpaceId, ProviderId, TargetSeq) ->
    InitialProviderParams = #synchronization_params{
        target_seq = TargetSeq,
        included_mutators = ?ALL_MUTATORS_EXCEPT_SENDER,
        mode = initial_sync
    },
    Default = #dbsync_state{seq = #{ProviderId => {1, 0}}, synchronization_params = #{ProviderId => InitialProviderParams}},
    DiffFun = fun(#dbsync_state{seq = Seq, synchronization_params = Params} = State) ->
        ProviderParams = maps:get(ProviderId, Params, undefined),
        case {maps:get(ProviderId, Seq, {1, 0}), ProviderParams} of
            {{1, 0}, undefined} ->
                {ok, State#dbsync_state{
                    synchronization_params = Params#{ProviderId => InitialProviderParams}
                }};
            _ ->
                {error, {nothing_changed, ProviderParams}}
        end
    end,

    case datastore_model:update(?CTX, SpaceId, DiffFun, Default) of
        {ok, #document{value = #dbsync_state{synchronization_params = UpdatedParams}}} ->
            maps:get(ProviderId, UpdatedParams, undefined);
        {error, {nothing_changed, Value}} ->
            Value
    end.


-spec set_seq_and_timestamp_internal(state(), od_provider:id(), couchbase_changes:seq(), dbsync_changes:timestamp()) ->
    state().
set_seq_and_timestamp_internal(
    #dbsync_state{seq = SeqMap, synchronization_params = Params} = State,
    ProviderId,
    NewSeq,
    Timestamp
) ->
    {UpdatedSeqMap, UpdatedParams} = case maps:get(ProviderId, Params, undefined) of
        #synchronization_params{
            mode = initial_sync_to_repeat, target_seq = TargetSeq
        } = ProviderParams when NewSeq >= TargetSeq ->
            {
                SeqMap#{ProviderId => {1, 0}},
                Params#{ProviderId => ProviderParams#synchronization_params{mode = resynchronization}}
            };
        #synchronization_params{target_seq = TargetSeq, seq_with_timestamp_to_restore = ToRestore} when NewSeq >= TargetSeq ->
            {SeqMap#{ProviderId => utils:ensure_defined(ToRestore, {NewSeq, Timestamp})}, maps:remove(ProviderId, Params)};
        _ ->
            {SeqMap#{ProviderId => {NewSeq, Timestamp}}, Params}
    end,
    State#dbsync_state{
        seq = UpdatedSeqMap,
        synchronization_params = UpdatedParams
    }.


-spec is_any_synchronization_in_mode(synchronization_mode(), synchronization_params_map()) -> boolean().
is_any_synchronization_in_mode(ExpectedMode, SynchronizationParams) ->
    ParamsToCheckValues = maps:values(SynchronizationParams),
    lists:any(fun(#synchronization_params{mode = Mode}) ->
        Mode =:= ExpectedMode
    end, ParamsToCheckValues).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    5.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {seq, #{string => integer}}
    ]};
get_record_struct(2) ->
    {record, [
        {seq, #{string => {integer, integer}}}
    ]};
get_record_struct(3) ->
    {record, [
        {seq, #{string => {integer, integer}}},
        {resynchronization_params, #{string => {record, [
            {target_seq, integer},
            {included_mutators, [string]}
        ]}}}
    ]};
get_record_struct(4) ->
    {record, [
        {seq, #{string => {integer, integer}}},
        {synchronization_params, #{string => {record, [
            {mode, atom}, % New field
            {target_seq, integer},
            {included_mutators, [string]}
        ]}}}
    ]};
get_record_struct(5) ->
    {record, [
        {seq, #{string => {integer, integer}}},
        {synchronization_params, #{string => {record, [
            {mode, atom},
            {target_seq, integer},
            {included_mutators, [string]},
            {seq_with_timestamp_to_restore, {integer, integer}} % New field
        ]}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Map}) ->
    Map2 = maps:map(fun(_ProviderId, Number) -> {Number, 0} end, Map),
    {2, {?MODULE, Map2}};
upgrade_record(2, {?MODULE, SeqMap}) ->
    {3, {?MODULE, SeqMap, #{}}};
upgrade_record(3, {?MODULE, SeqMap, Params}) ->
    {4, {?MODULE, SeqMap, maps:map(fun(_ProvideId, {resynchronization_params, TargetSeq, IncludedMutators}) ->
        {synchronization_params, resynchronization, TargetSeq, IncludedMutators}
    end, Params)}};
upgrade_record(4, {?MODULE, SeqMap, Params}) ->
    {5, {?MODULE, SeqMap, maps:map(fun(_ProvideId, {synchronization_params, Mode, TargetSeq, IncludedMutators}) ->
        {synchronization_params, Mode, TargetSeq, IncludedMutators, undefined}
    end, Params)}}.