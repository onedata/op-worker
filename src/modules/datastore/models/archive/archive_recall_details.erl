%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing details of archive recalls. 
%%% This model is synchronized between providers.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_details).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").


%% API
-export([create/2, delete/1]).
-export([get/1]).
-export([report_started/1, report_finished/1, report_cancel_started/1, report_error/2]).
%% Datastore callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, 
    on_remote_doc_created/2, resolve_conflict/3]).
%% DBsync events hooks
-export([handle_remote_change/2]).

-type id() :: archive_recall:id().
-type doc() :: datastore_doc:doc(record()).
-type record() :: #archive_recall_details{}.

-export_type([record/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(id(), archive:doc()) -> ok | {error, term()}.
create(Id, ArchiveDoc) ->
    {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    {ok, Stats} = archive_api:get_aggregated_stats(ArchiveDoc),
    ?extract_ok(datastore_model:create(?CTX, #document{
        key = Id,
        scope = SpaceId,
        value = #archive_recall_details{
            recalling_provider_id = oneprovider:get_id(),
            archive_id = ArchiveId,
            dataset_id = DatasetId,
            total_byte_size = archive_stats:get_archived_bytes(Stats),
            total_file_count = archive_stats:get_archived_files(Stats)
        }}
    )).


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    datastore_model:delete(?CTX, Id).


-spec get(id()) -> {ok, record()} | {error, term()}.
get(Id) ->
    case datastore_model:get(?CTX, Id) of
        {ok, #document{value = Record}} ->
            {ok, Record};
        Error ->
            Error
    end.


-spec report_started(id()) -> ok | {error, term()}.
report_started(Id) ->
    ?extract_ok(datastore_model:update(?CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall_details{start_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_finished(id()) -> ok | {error, term()}.
report_finished(Id) ->
    ?extract_ok(datastore_model:update(?CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall_details{finish_timestamp = global_clock:timestamp_millis()}}
    end)).


-spec report_cancel_started(id()) -> ok | {error, term()}.
report_cancel_started(Id) ->
    ?extract_ok(datastore_model:update(?CTX, Id, fun
        (#archive_recall_details{cancel_timestamp = undefined} = ArchiveRecall) -> 
            {ok, ArchiveRecall#archive_recall_details{cancel_timestamp = global_clock:timestamp_millis()}};
        (ArchiveRecall) -> 
            {ok, ArchiveRecall}
    end)).


-spec report_error(id(), json_utils:json_term()) -> ok | {error, term()}.
report_error(Id, ErrorJson) ->
    ?extract_ok(datastore_model:update(?CTX, Id, fun(ArchiveRecall) ->
        {ok, ArchiveRecall#archive_recall_details{last_error = ErrorJson}}
    end)).


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    %%% WARNING: this is a synced model and MUST NOT be changed outside of a new major release!!!
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    1.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    %%% WARNING: this is a synced model and MUST NOT be changed outside of a new major release!!!
    %%% !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    {record, [
        {recalling_provider_id, string},
        {archive_id, string},
        {dataset_id, string},
        {start_timestamp, integer},
        {finish_timestamp, integer},
        {cancel_timestamp, integer},
        {total_files, integer},
        {total_bytes, integer},
        {last_error, {custom, json, {json_utils, encode, decode}}}
    ]}.


-spec on_remote_doc_created(datastore_model:ctx(), doc()) -> ok.
on_remote_doc_created(_Ctx, #document{deleted = true}) ->
    ok;
on_remote_doc_created(_Ctx, #document{scope = SpaceId}) ->
    archive_recall_cache:invalidate_on_all_nodes(SpaceId).


-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, #document{value = RemoteValue} = RemoteDoc, #document{value = LocalValue} = LocalDoc) ->
    % Only cancel_timestamp field can be changed by any provider. 
    % All other changes are done by recalling provider.
    
    #document{revs = [LocalRev | _]} = RemoteDoc,
    #document{revs = [RemoteRev | _]} = LocalDoc,
    
    #archive_recall_details{
        recalling_provider_id = RecallingProviderId, 
        cancel_timestamp = LocalCancelTimestamp
    } = LocalValue,
    #archive_recall_details{cancel_timestamp = RemoteCancelTimestamp} = RemoteValue,
    LocalProviderId = oneprovider:get_id_or_undefined(),
    #document{mutators = [RemoteDocMutator]} = RemoteDoc,
    
    case {datastore_rev:is_greater(RemoteRev, LocalRev), RecallingProviderId, RemoteCancelTimestamp < LocalCancelTimestamp} of
        {_IsRemoteRevGreater = true, LocalProviderId, _IsRemoteCancelEarlier = true} ->
            {true, RemoteDoc#document{
                value = LocalValue#archive_recall_details{cancel_timestamp = RemoteCancelTimestamp}
            }};
        {_IsRemoteRevGreater = true, LocalProviderId, _IsRemoteCancelEarlier = false} ->
            {true, RemoteDoc#document{
                value = LocalValue
            }};
        {_IsRemoteRevGreater = true, _, _} when RemoteCancelTimestamp =< LocalCancelTimestamp ->
            {false, RemoteDoc};
        {_IsRemoteRevGreater = true, _, _IsRemoteCancelEarlier = false} ->
            {true, RemoteDoc#document{
                value = RemoteValue#archive_recall_details{cancel_timestamp = LocalCancelTimestamp}
            }};
        {_IsRemoteRevGreater = false, LocalProviderId, _IsRemoteCancelEarlier = true} ->
            {true, LocalDoc#document{
                value = LocalValue#archive_recall_details{cancel_timestamp = RemoteCancelTimestamp}
            }};
        {_IsRemoteRevGreater = false, LocalProviderId, _IsRemoteCancelEarlier = false} ->
            ignore;
        {_IsRemoteRevGreater = false, RemoteDocMutator, _IsRemoteCancelEarlier = true} ->
            {true, LocalDoc#document{
                value = RemoteValue
            }};
        {_IsRemoteRevGreater = false, RemoteDocMutator, _IsRemoteCancelEarlier = false} ->
            {true, LocalDoc#document{
                value = RemoteValue#archive_recall_details{cancel_timestamp = LocalCancelTimestamp}
            }};
        {_IsRemoteRevGreater = false, __, _IsRemoteCancelEarlier = true} ->
            {true, LocalDoc#document{
                value = LocalValue#archive_recall_details{cancel_timestamp = RemoteCancelTimestamp}
            }};
        {_IsRemoteRevGreater = false, __, _IsRemoteCancelEarlier = false} ->
            ignore
    end.


%%%===================================================================
%%% DBsync events hooks
%%%===================================================================

-spec handle_remote_change(od_space:id(), doc()) -> ok.
handle_remote_change(_SpaceId, #document{value = #archive_recall_details{finish_timestamp = undefined}}) ->
    ok;
handle_remote_change(SpaceId, #document{value = #archive_recall_details{recalling_provider_id = ProviderId}}) ->
    case oneprovider:get_id_or_undefined() of
        ProviderId ->
            ok;
        _ ->
            % It should only be invalidated after changes done by recalling provider but
            % it is not possible to unambiguously determine change origin provider 
            % (mutator field could have been overwritten during conflict resolution).
            % Therefore invalidate cache on each remote change 
            % (recall cancels should be rare, so it should not be a problem).
            archive_recall_cache:invalidate_on_all_nodes(SpaceId)
    end.
