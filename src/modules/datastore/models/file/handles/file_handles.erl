%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that stores open files.
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, all hardlinks pointing on
%%% the same file share single file_handles document. It is necessary because
%%% file can be deleted from storage system only after deletion of last hardlink.
%%% @end
%%%-------------------------------------------------------------------
-module(file_handles).
-author("Michal Wrona").
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_file_opened/1, delete/1, list/0]).
-export([register_open/4, register_release/3, mark_to_remove/2, is_removed/1, get_removal_status/1,
    invalidate_session_entry/2, is_used_by_session/2, get_creation_handle/1]).
-export([gen_handle_id/1, get_open_flag/1]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type key() :: datastore:key().
-type record() :: #file_handles{}.
-type doc() :: datastore_doc:doc(record()).
% Handle created during file creation.
% Read/write with this handle should be allowed even if file permissions forbid them.
-type creation_handle() :: file_req:handle_id().
-type removal_status() :: ?NOT_REMOVED | ?LOCAL_REMOVE | ?REMOTE_REMOVE.

-export_type([creation_handle/0, removal_status/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    local_fold => true
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_file_opened(key()) -> boolean().
is_file_opened(FileUuid) ->
    case datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, #document{value = #file_handles{descriptors = Fds}}} ->
            maps:size(Fds) =/= 0;
        {error, not_found} ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes file handle.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, fslogic_file_id:ensure_referenced_uuid(Key)).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:local_fold_all_nodes(?CTX, fun(Doc, InternalAcc) -> {ok, [Doc | InternalAcc]} end, []).

-spec is_removed(record() | doc()) -> boolean().
is_removed(#document{value = FileHandles}) ->
    is_removed(FileHandles);
is_removed(#file_handles{removal_status = RemovalStatus}) ->
    RemovalStatus =/= ?NOT_REMOVED.


-spec get_removal_status(record() | doc()) -> removal_status().
get_removal_status(#document{value = FileHandles}) ->
    get_removal_status(FileHandles);
get_removal_status(#file_handles{removal_status = RemovalStatus}) ->
    RemovalStatus.

%%--------------------------------------------------------------------
%% @doc
%% Registers number given in Count of new file descriptors for given
%% FileCtx and SessionId.
%% @end
%%--------------------------------------------------------------------
-spec register_open(file_ctx:ctx(), session:id(), pos_integer(), creation_handle()) ->
    ok | {error, term()}.
register_open(FileCtx, SessId, Count, CreateHandleID) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    FileGuid = file_ctx:get_referenced_guid_const(FileCtx),
    Diff = fun
        (#file_handles{removal_status = RemovalStatus}) when RemovalStatus =/= ?NOT_REMOVED ->
            {error, removed};
        (Handle = #file_handles{descriptors = Fds}) ->
            FdCount = maps:get(SessId, Fds, 0),
            {ok, Handle#file_handles{
                descriptors = maps:put(SessId, FdCount + Count, Fds)
            }}
    end,
    Diff2 = fun
        (Handle = #file_handles{descriptors = Fds}) ->
            case {maps:get(SessId, Fds, 0) - Count, CreateHandleID} of
                {0, undefined} ->
                    {ok, Handle#file_handles{
                        descriptors = maps:remove(SessId, Fds)
                    }};
                {0, _} ->
                    {ok, Handle#file_handles{
                        descriptors = maps:remove(SessId, Fds),
                        creation_handle = CreateHandleID
                    }};
                {FdCount, undefined} ->
                    {ok, Handle#file_handles{
                        descriptors = maps:put(SessId, FdCount, Fds)
                    }};
                {FdCount, _} ->
                    {ok, Handle#file_handles{
                        descriptors = maps:put(SessId, FdCount, Fds),
                        creation_handle = CreateHandleID
                    }}
            end
    end,
    Default = #document{key = FileUuid, value = #file_handles{
        descriptors = #{SessId => Count},
        creation_handle = CreateHandleID
    }},
    case datastore_model:update(?CTX, FileUuid, Diff, Default) of
        {ok, _} ->
            case session_open_files:register(SessId, FileGuid) of
                ok ->
                    ok;
                {error, Reason} ->
                    datastore_model:update(?CTX, FileUuid, Diff2),
                    {error, Reason}
            end;
        {error, removed} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes number given in Count of file descriptors for given
%% FileUuid and SessionId. Removes file if no file descriptor
%% is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec register_release(file_ctx:ctx(), session:id(), pos_integer() | infinity) ->
    ok | {error, term()}.
register_release(FileCtx, SessId, Count) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    FileGuid = file_ctx:get_referenced_guid_const(FileCtx),
    Diff = fun(Handle = #file_handles{removal_status = RemovalStatus, descriptors = Fds}) ->
        FdCount = maps:get(SessId, Fds, 0),
        case Count =:= infinity orelse FdCount =< Count of
            true ->
                Fds2 = maps:remove(SessId, Fds),
                case {RemovalStatus =/= ?NOT_REMOVED, maps:size(Fds2)} of
                    {true, 0} -> {error, {removed, RemovalStatus}};
                    _ -> {ok, Handle#file_handles{descriptors = Fds2}}
                end;
            false ->
                {ok, Handle#file_handles{
                    descriptors = maps:put(SessId, FdCount - Count, Fds)
                }}
        end
    end,
    case datastore_model:update(?CTX, FileUuid, Diff) of
        {ok, #document{value = #file_handles{descriptors = Fds}}} ->
            case maps:is_key(SessId, Fds) of
                true -> ok;
                false ->
                    % TODO VFS-6153 race with open
                    session_open_files:deregister(SessId, FileGuid)
            end,
            Pred = fun(#file_handles{descriptors = Fds2}) ->
                maps:size(Fds2) == 0
            end,
            case datastore_model:delete(?CTX, FileUuid, Pred) of
                ok -> ok;
                {error, {not_satisfied, _}} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, {removed, RemovalStatus}} ->
            session_open_files:deregister(SessId, FileGuid),
            fslogic_delete:handle_release_of_deleted_file(FileCtx, RemovalStatus),
            datastore_model:delete(?CTX, FileUuid);
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks files as removed.
%% @end
%%--------------------------------------------------------------------
-spec mark_to_remove(file_ctx:ctx(), removal_status()) -> ok | {error, term()}.
mark_to_remove(FileCtx, RemovalStatus) ->
    Diff = fun
        (Handle = #file_handles{removal_status = ?NOT_REMOVED}) ->
            {ok, Handle#file_handles{removal_status = RemovalStatus}};
        (_) ->
            {error, already_removed}
    end,
    case datastore_model:update(?CTX, file_ctx:get_referenced_uuid_const(FileCtx), Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Clears descriptors count associated with SessionId for given FileUuid.
%% Removes file if no file descriptor is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_session_entry(file_ctx:ctx(), session:id()) ->
    ok | {error, term()}.
invalidate_session_entry(FileCtx, SessId) ->
    case register_release(FileCtx, SessId, infinity) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns information if file is opened by particular session.
%% @end
%%--------------------------------------------------------------------
-spec is_used_by_session(file_ctx:ctx(), session:id()) -> boolean() | {error, term()}.
is_used_by_session(FileCtx, SessId) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case datastore_model:get(?CTX, FileUuid) of
        {ok, #document{value = #file_handles{descriptors = Fds}}} -> maps:is_key(SessId, Fds);
        {error, not_found} -> false;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns handle connected with file creation.
%% @end
%%--------------------------------------------------------------------
-spec get_creation_handle(key()) -> {ok, creation_handle()} | {error, term()}.
get_creation_handle(Key) ->
    case datastore_model:get(?CTX, fslogic_file_id:ensure_referenced_uuid(Key)) of
        {ok, #document{value = #file_handles{creation_handle = Handle}}} -> {ok, Handle};
        Other -> Other
    end.


-spec gen_handle_id(fslogic_worker:open_flag()) -> storage_driver:handle_id().
gen_handle_id(Flag) ->
    <<(atom_to_binary(Flag, utf8))/binary, "_", (base64:encode(crypto:strong_rand_bytes(20)))/binary>>.


-spec get_open_flag(storage_driver:handle_id()) -> fslogic_worker:open_flag().
get_open_flag(HandleId) ->
    [FlagBin | _] = binary:split(HandleId, <<"_">>),
    binary_to_atom(FlagBin, utf8).



%%%===================================================================
%%% datastore_model callbacks
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
    4.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {is_removed, boolean},
        {active_descriptors, #{string => integer}}
    ]};
get_record_struct(2) ->
    {record, Struct} = get_record_struct(1),
    {record, lists:keyreplace(
        active_descriptors, 1, Struct, {descriptors, #{string => integer}}
    )};
get_record_struct(3) ->
    {record, [
        {is_removed, boolean},
        {descriptors, #{string => integer}},
        {creation_handle, binary}
    ]};
get_record_struct(4) ->
    {record, [
        % Field is_removed was replaced by removal_status
        % Its type was changed from boolean to atom.
        {removal_status, atom},
        {descriptors, #{string => integer}},
        {creation_handle, binary}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, IsRemoved, Descriptors}) ->
    {2, {?MODULE, IsRemoved, Descriptors}};
upgrade_record(2, {?MODULE, IsRemoved, Descriptors}) ->
    {3, {?MODULE, IsRemoved, Descriptors, undefined}};
upgrade_record(3, {?MODULE, IsRemoved, Descriptors, CreationHandle}) ->
    RemovalStatus = case IsRemoved of
        false -> ?NOT_REMOVED;
        true -> ?LOCAL_REMOVE
    end,
    {4, {?MODULE, RemovalStatus, Descriptors, CreationHandle}}.