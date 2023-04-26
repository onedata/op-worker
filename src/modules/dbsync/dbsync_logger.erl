%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains logger for dbsync.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_logger).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").


%% API
-export([log_apply/5, log_batch_received/5, log_batch_requested/4, log_batch_sending/4]).


-define(CHANGES_FILE_MAX_SIZE, op_worker:get_env(dbsync_changes_audit_log_file_max_size, 104857600)). % 100 MB
-define(OUT_STREAM_FILE_MAX_SIZE, op_worker:get_env(dbsync_out_stream_audit_log_file_max_size, 104857600)). % 100 MB
-define(CHANGES_FILE_PREFIX, op_worker:get_env(dbsync_changes_audit_log_file_prefix, "/tmp/dbsync_changes_")).
-define(OUT_STREAM_FILE_PREFIX, op_worker:get_env(dbsync_out_stream_audit_log_file_prefix, "/tmp/dbsync_out_stream_")).

%%%===================================================================
%%% API
%%%===================================================================

-spec log_apply(
    [datastore:doc()],
    {couchbase_changes:since(), couchbase_changes:until()},
    ok | timeout | {error, datastore_doc:seq(), term()},
    od_space:id(),
    od_provider:id()
) -> ok.
log_apply(Docs, BatchRange, Ans, SpaceId, ProviderId) ->
    case ?CHANGES_FILE_MAX_SIZE of
        0 ->
            ok;
        MaxSize ->
            Seqs = lists:map(fun(#document{seq = Seq}) -> Seq end, Docs),
            Log = "Seqs range ~p applied with ans: ~p~nSeqs in range: ~w",
            Args = [BatchRange, Ans, Seqs],
            onedata_logger:log_with_rotation(get_changes_log_file(SpaceId, ProviderId), Log, Args, MaxSize)
    end.


-spec log_batch_received(couchbase_changes:since(), couchbase_changes:until(), couchbase_changes:seq(),
    od_space:id(), od_provider:id()) -> ok.
log_batch_received(Seq, Seq, _CurrentSeq, _SpaceId, _ProviderId) ->
    ok;
log_batch_received(Since, Until, CurrentSeq, SpaceId, ProviderId) ->
    case ?CHANGES_FILE_MAX_SIZE of
        0 ->
            ok;
        MaxSize ->
            Log = "Seqs range ~p received, current seq ~p",
            Args = [{Since, Until}, CurrentSeq],
            onedata_logger:log_with_rotation(get_changes_log_file(SpaceId, ProviderId), Log, Args, MaxSize)
    end.


-spec log_batch_requested(couchbase_changes:since(), couchbase_changes:until(), od_space:id(), od_provider:id()) -> ok.
log_batch_requested(Since, Until, SpaceId, ProviderId) ->
    case ?CHANGES_FILE_MAX_SIZE of
        0 ->
            ok;
        MaxSize ->
            Log = "Seqs range ~p requested",
            Args = [{Since, Until}],
            onedata_logger:log_with_rotation(get_changes_log_file(SpaceId, ProviderId), Log, Args, MaxSize)
    end.


-spec log_batch_sending(couchbase_changes:since(), couchbase_changes:until(), od_provider:id() | all, od_space:id()) -> ok.
log_batch_sending(Since, Until, ProviderId, SpaceId) ->
    case ?OUT_STREAM_FILE_MAX_SIZE of
        0 ->
            ok;
        MaxSize ->
            LogFile = ?OUT_STREAM_FILE_PREFIX ++ str_utils:to_list(SpaceId) ++ ".log",

            Log = "Seqs range ~p sent to ~p",
            Args = [{Since, Until}, ProviderId],
            onedata_logger:log_with_rotation(LogFile, Log, Args, MaxSize)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_changes_log_file(od_space:id(), od_provider:id()) -> string().
get_changes_log_file(SpaceId, ProviderId) ->
    ?CHANGES_FILE_PREFIX ++ str_utils:to_list(SpaceId) ++ "_" ++ str_utils:to_list(ProviderId) ++ ".log".