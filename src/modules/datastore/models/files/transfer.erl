%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about ongoing transfer. Creation of doc works as a
%%% trigger for starting a transfer or replica invalidation.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start/6, stop/1, get_status/1, get_info/1]).
-export([mark_active/1, mark_completed/1, mark_file_transfer_scheduled/2,
    mark_file_transfer_finished/2, mark_data_transfer_scheduled/2,
    mark_data_transfer_finished/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-type id() :: binary().
-type status() :: scheduled | skipped | active | completed | cancelled | failed.
-type callback() :: undefined | binary().
-type transfer() :: #transfer{}.
-type doc() :: #document{value :: transfer()}.

-export_type([id/0, status/0, callback/0, doc/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {file_uuid, string},
        {space_id, string},
        {path, string},
        {session_id, string},
        {callback, string},
        {transfer_status, atom},
        {invalidation_status, atom},
        {source_provider_id, string},
        {target_provider_id, string},
        {invalidate_source_replica, boolean},
        {files_to_transfer, integer},
        {files_transferred, integer},
        {bytes_to_transfer, integer},
        {bytes_transferred, integer}
    ]}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start(session:id(), fslogic_worker:file_guid(), file_meta:path(),
    oneprovider:id(), binary(), boolean()) -> {ok, id()} | ignore | {error, Reason :: term()}.
start(SessionId, FileGuid, FilePath, ProviderId, Callback, InvalidateSourceReplica) ->
    TransferStatus = case ProviderId of
        undefined ->
            skipped;
        _ ->
            scheduled
    end,
    InvalidationStatus = case InvalidateSourceReplica of
        true ->
            scheduled;
        false ->
            skipped
    end,
    ToCreate = #document{
        scope = fslogic_uuid:guid_to_space_id(FileGuid),
        value = #transfer{
            file_uuid = fslogic_uuid:guid_to_uuid(FileGuid),
            space_id = fslogic_uuid:guid_to_space_id(FileGuid),
            path = FilePath,
            callback = Callback,
            session_id = SessionId,
            source_provider_id = oneprovider:get_provider_id(),
            target_provider_id = ProviderId,
            transfer_status = TransferStatus,
            invalidation_status = InvalidationStatus,
            invalidate_source_replica = InvalidateSourceReplica
        }},
    {ok, TransferId} = create(ToCreate),
    session:add_transfer(SessionId, TransferId),
    transfer_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    invalidation_controller:on_new_transfer_doc(ToCreate#document{key = TransferId}),
    {ok, TransferId}.

%%--------------------------------------------------------------------
%% @doc
%% Gets status of the transfer
%% @end
%%--------------------------------------------------------------------
-spec get_status(TransferId :: id()) -> status().
get_status(TransferId) ->
    {ok, #document{value = #transfer{transfer_status = Status}}} = get(TransferId),
    Status.

%%--------------------------------------------------------------------
%% @doc
%% Gets transfer info
%% @end
%%--------------------------------------------------------------------
-spec get_info(TransferId :: id()) -> maps:map().
get_info(TransferId) ->
    {ok, #document{value = #transfer{
        file_uuid = FileUuid,
        space_id = SpaceId,
        path = Path,
        transfer_status = TransferStatus,
        invalidation_status = InvalidationStatus,
        target_provider_id = TargetProviderId,
        callback = Callback,
        files_to_transfer = FilesToTransfer,
        files_transferred = FilesTransferred,
        bytes_to_transfer = BytesToTransfer,
        bytes_transferred = BytesTransferred
    }}} = get(TransferId),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
    NullableCallback = utils:ensure_defined(Callback, undefined, null),
    {ok, FileObjectId} = cdmi_id:guid_to_objectid(FileGuid),
    #{
        <<"fileId">> => FileObjectId,
        <<"path">> => Path,
        <<"transferStatus">> => atom_to_binary(TransferStatus, utf8),
        <<"invalidationStatus">> => atom_to_binary(InvalidationStatus, utf8),
        <<"targetProviderId">> => TargetProviderId,
        <<"callback">> => NullableCallback,
        <<"filesToTransfer">> => FilesToTransfer,
        <<"filesTransferred">> => FilesTransferred,
        <<"bytesToTransfer">> => BytesToTransfer,
        <<"bytesTransferred">> => BytesTransferred
    }.

%%--------------------------------------------------------------------
%% @doc
%% Stop transfer
%% @end
%%--------------------------------------------------------------------
-spec stop(id()) -> ok.
stop(TransferId) ->
    delete(TransferId).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as active and sets number of files to transfer to 1.
%% @end
%%--------------------------------------------------------------------
-spec mark_active(id()) -> ok.
mark_active(TransferId) ->
    transfer:update(TransferId, #{transfer_status => active, files_to_transfer => 1}).

%%--------------------------------------------------------------------
%% @doc
%% Marks transfer as completed
%% @end
%%--------------------------------------------------------------------
-spec mark_completed(id()) -> ok.
mark_completed(TransferId) ->
    transfer:update(TransferId, #{transfer_status => completed}).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'FilesNum' files are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_transfer_scheduled(id(), non_neg_integer()) -> ok.
mark_file_transfer_scheduled(TransferId, FilesNum) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_to_transfer = Transfer#transfer.files_to_transfer + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'FilesNum' files.
%% @end
%%--------------------------------------------------------------------
-spec mark_file_transfer_finished(id(), non_neg_integer()) -> ok.
mark_file_transfer_finished(TransferId, FilesNum) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            files_transferred = Transfer#transfer.files_transferred + FilesNum
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc that 'Bytes' bytes are scheduled to be transferred.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_scheduled(id(), non_neg_integer()) -> ok.
mark_data_transfer_scheduled(TransferId, Bytes) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            bytes_to_transfer = Transfer#transfer.bytes_to_transfer + Bytes
        }}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks in transfer doc successful transfer of 'Bytes' bytes.
%% @end
%%--------------------------------------------------------------------
-spec mark_data_transfer_finished(id(), non_neg_integer()) -> ok.
mark_data_transfer_finished(TransferId, Bytes) ->
    transfer:update(TransferId, fun(Transfer) ->
        {ok, Transfer#transfer{
            bytes_transferred = Transfer#transfer.bytes_transferred + Bytes
        }}
    end).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(transfer_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 1, sync_enabled = true}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================