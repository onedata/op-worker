%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_transfer model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_transfer).
-author("Bartosz Walkowicz").

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_transfer.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, rerun, private) -> true;

operation_supported(get, instance, private) -> true;

operation_supported(delete, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = rerun}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:entity()} | op_logic:error().
fetch_entity(#op_req{gri = #gri{id = TransferId}}) ->
    case transfer:get(TransferId) of
        {ok, #document{value = Transfer}} ->
            % Transfer doc is synchronized only with providers supporting space
            % so if it was fetched then space must be supported locally
            {ok, Transfer};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), op_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), op_logic:entity()) -> boolean().
authorize(#op_req{client = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, client = ?USER(UserId), gri = #gri{
    aspect = rerun
}}, #transfer{space_id = SpaceId} = Transfer) ->
    case transfer:type(Transfer) of
        undefined ->
            ?ERROR_FORBIDDEN;
        replication ->
            space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_SCHEDULE_REPLICATION);
        eviction ->
            space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_SCHEDULE_EVICTION);
        migration ->
            space_logic:has_eff_privileges(
                SpaceId, UserId, [?SPACE_SCHEDULE_REPLICATION, ?SPACE_SCHEDULE_EVICTION]
            )
    end;

authorize(#op_req{operation = get, client = ?USER(UserId), gri = #gri{
    aspect = instance
}}, #transfer{space_id = SpaceId}) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_TRANSFERS);

authorize(#op_req{operation = delete, client = ?USER(UserId), gri = #gri{
    aspect = instance
}} = Req, #transfer{space_id = SpaceId} = Transfer) ->
    case Transfer#transfer.user_id of
        UserId ->
            % User doesn't need cancel privileges to cancel his transfer but
            % must still be member of space.
            op_logic_utils:is_eff_space_member(Req#op_req.client, SpaceId);
        _ ->
            case transfer:type(Transfer) of
                undefined ->
                    ?ERROR_FORBIDDEN;
                replication ->
                    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_CANCEL_REPLICATION);
                eviction ->
                    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_CANCEL_EVICTION);
                migration ->
                    space_logic:has_eff_privileges(
                        SpaceId, UserId, [?SPACE_CANCEL_REPLICATION, ?SPACE_CANCEL_EVICTION]
                    )
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback validate/2.
%%
%% Does not check if space is locally supported because if it wasn't
%% it would not be possible to fetch transfer doc (it is synchronized
%% only between providers supporting given space).
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), op_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{aspect = rerun}}, _) ->
    ok;

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    ok;

validate(#op_req{operation = delete, gri = #gri{aspect = instance}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, gri = #gri{id = TransferId, aspect = rerun}}) ->
    case transfer:rerun_ended(Cl#client.id, TransferId) of
        {ok, NewTransferId} ->
            {ok, value, NewTransferId};
        {error, not_ended} ->
            ?ERROR_TRANSFER_NOT_ENDED;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{gri = #gri{aspect = instance}}, Transfer) ->
    {ok, transfer_to_json(Transfer)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link op_logic_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{gri = #gri{id = TransferId, aspect = instance}}) ->
    case transfer:cancel(TransferId) of
        ok ->
            ok;
        {error, already_ended} ->
            ?ERROR_TRANSFER_ALREADY_ENDED;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec transfer_to_json(transfer:transfer()) -> maps:map().
transfer_to_json(#transfer{
    file_uuid = FileUuid,
    space_id = SpaceId,
    user_id = UserId,
    rerun_id = RerunId,
    path = Path,
    replication_status = ReplicationStatus,
    eviction_status = EvictionStatus,
    evicting_provider = EvictingProvider,
    replicating_provider = ReplicatingProviderId,
    callback = Callback,
    files_to_process = FilesToProcess,
    files_processed = FilesProcessed,
    failed_files = FailedFiles,
    files_replicated = FilesReplicated,
    bytes_replicated = BytesReplicated,
    files_evicted = FilesEvicted,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime,
    last_update = LastUpdate,
    min_hist = MinHist,
    hr_hist = HrHist,
    dy_hist = DyHist,
    mth_hist = MthHist
}) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ReplicationStatusBin = atom_to_binary(ReplicationStatus, utf8),
    ReplicatingProvider = utils:ensure_defined(
        ReplicatingProviderId, undefined, null
    ),

    #{
        <<"fileId">> => FileObjectId,
        <<"userId">> => UserId,
        <<"rerunId">> => utils:ensure_defined(RerunId, undefined, null),
        <<"path">> => Path,
        <<"transferStatus">> => ReplicationStatusBin,
        <<"replicationStatus">> => ReplicationStatusBin,
        <<"invalidationStatus">> => atom_to_binary(EvictionStatus, utf8),
        <<"replicaEvictionStatus">> => atom_to_binary(EvictionStatus, utf8),
        <<"targetProviderId">> => ReplicatingProvider,
        <<"replicatingProviderId">> => ReplicatingProvider,
        <<"evictingProviderId">> => utils:ensure_defined(
            EvictingProvider, undefined, null
        ),
        <<"callback">> => NullableCallback,
        <<"filesToProcess">> => FilesToProcess,
        <<"filesProcessed">> => FilesProcessed,
        <<"filesTransferred">> => FilesReplicated,
        <<"filesReplicated">> => FilesReplicated,
        <<"failedFiles">> => FailedFiles,
        <<"filesInvalidated">> => FilesEvicted,
        <<"fileReplicasEvicted">> => FilesEvicted,
        <<"bytesTransferred">> => BytesReplicated,
        <<"bytesReplicated">> => BytesReplicated,
        <<"scheduleTime">> => ScheduleTime,
        <<"startTime">> => StartTime,
        <<"finishTime">> => FinishTime,
        % It is possible that there is no last update, if 0 bytes were
        % transferred, in this case take the start time.
        <<"lastUpdate">> => lists:max([StartTime | maps:values(LastUpdate)]),
        <<"minHist">> => MinHist,
        <<"hrHist">> => HrHist,
        <<"dyHist">> => DyHist,
        <<"mthHist">> => MthHist
    }.
