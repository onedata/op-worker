%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Files that need to be chowned when their owner shows in provider.
%%% @end
%%%-------------------------------------------------------------------
-module(files_to_chown).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chown_or_defer/1, chown_deferred_files/1]).
-export([get/1, delete/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, get_ctx/0]).

-type id() :: od_user:id().
-type record() :: #files_to_chown{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-export_type([id/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% If given UserId is present in provider, then file owner is changed.
%% Otherwise, file is added to files awaiting owner change.
%% @end
%%--------------------------------------------------------------------
-spec chown_or_defer(file_ctx:ctx()) -> file_ctx:ctx().
chown_or_defer(FileCtx) ->
    {Storage, FileCtx2} = file_ctx:get_storage(FileCtx),
    % TODO VFS-3868 implement chown in other helpers and remove this case
    case Storage =/= undefined andalso storage:is_posix_compatible(Storage) of
        true -> chown_or_defer_on_posix_compatible_storage(FileCtx2);
        false -> FileCtx2
    end.

%%--------------------------------------------------------------------
%% @doc
%% Chown all deferred files of given user
%% @end
%%--------------------------------------------------------------------
-spec chown_deferred_files(od_user:id()) -> ok.
chown_deferred_files(UserId) ->
    case files_to_chown:get(UserId) of
        {ok, #document{value = #files_to_chown{file_guids = FileGuids}}} ->
            lists:foreach(fun chown_deferred_file/1, FileGuids),
            delete(UserId);
        {error, not_found} ->
            ok
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec create_or_update(id(), diff()) -> ok | {error, term()}.
create_or_update(UserId, Diff) ->
    {ok, Default} = Diff(#files_to_chown{}),
    ?extract_ok(datastore_model:update(?CTX, UserId, Diff, Default)).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).


-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec chown_or_defer_on_posix_compatible_storage(file_ctx:ctx()) -> file_ctx:ctx().
chown_or_defer_on_posix_compatible_storage(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    OwnerUserId = file_meta:get_owner(FileDoc),
    % space_owner is a virtual user therefore we don't check whether it exists in Onezone
    case fslogic_uuid:is_space_owner(OwnerUserId)of
        true ->
            chown_file(FileCtx2, OwnerUserId);
        false ->
            case provider_logic:has_eff_user(OwnerUserId) of
                true ->
                    chown_file(FileCtx2, OwnerUserId);
                false ->
                    % possible cases:
                    %  * user was deleted, but is still owner of a file
                    %  * file was synced from storage, and through reverse luma
                    %    we received id of user that has not yet logged to Onezone
                    SpaceId = file_ctx:get_space_id_const(FileCtx2),
                    % temporarily chown file to ?SPACE_OWNER_ID so that it does not belong to root on storage
                    chown_file(FileCtx2, ?SPACE_OWNER_ID(SpaceId)),
                    ok = defer_chown(FileCtx2, OwnerUserId),
                    FileCtx2
            end
    end.

-spec chown_deferred_file(fslogic_worker:file_guid()) -> file_ctx:ctx().
chown_deferred_file(FileGuid) ->
    try
        FileCtx = file_ctx:new_by_guid(FileGuid),
        chown_file(FileCtx)
    catch
        _:Error ->
            ?error_stacktrace("Cannot chown deferred file ~p due to error ~p", [FileGuid, Error])
    end.

-spec chown_file(file_ctx:ctx()) -> file_ctx:ctx().
chown_file(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    OwnerId = file_meta:get_owner(FileDoc),
    chown_file(FileCtx2, OwnerId).


-spec chown_file(file_ctx:ctx(), od_user:id()) -> file_ctx:ctx().
chown_file(FileCtx, OwnerId) ->
    {SDHandle, FileCtx2} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx),
    {Storage, FileCtx3} = file_ctx:get_storage(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    {ok, StorageCredentials} = luma:map_to_storage_credentials(OwnerId, SpaceId, Storage),
    Uid = binary_to_integer(maps:get(<<"uid">>, StorageCredentials)),
    Gid = binary_to_integer(maps:get(<<"gid">>, StorageCredentials)),
    storage_driver:chown(SDHandle, Uid, Gid),
    FileCtx3.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add file that need to be chowned in future.
%% @end
%%--------------------------------------------------------------------
-spec defer_chown(file_ctx:ctx(), od_user:id()) -> ok | {error, term()}.
defer_chown(FileCtx, UserId) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    UpdateFun = fun(FTC = #files_to_chown{file_guids = Guids}) ->
        case lists:member(FileGuid, Guids) of
            true -> {ok, FTC};
            false -> {ok, FTC#files_to_chown{file_guids = [FileGuid | Guids]}}
        end
    end,
    create_or_update(UserId, UpdateFun).

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
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {file_uuids, [string]}
    ]};
get_record_struct(2) ->
    {record, [
        {file_guids, [string]}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Uuids}) ->
    Guids = lists:map(fun fslogic_uuid:uuid_to_guid/1, Uuids),
    {2, #files_to_chown{file_guids = Guids}}.