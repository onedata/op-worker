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
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chown_or_schedule_chowning/1, chown_file/1]).
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, create_or_update/2]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

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
%% If given UserId is present in provider, then file owner is changes.
%% Otherwise, file is added to files awaiting owner change.
%% @end
%%--------------------------------------------------------------------
-spec chown_or_schedule_chowning(file_ctx:ctx()) -> file_ctx:ctx().
chown_or_schedule_chowning(FileCtx) ->
    {#document{value = #file_meta{owner = OwnerUserId}}, FileCtx2} =
        file_ctx:get_file_doc(FileCtx),
    case od_user:exists(OwnerUserId) of
        true ->
            chown_file(FileCtx2);
        false ->
            {ok, _} = add(FileCtx2, OwnerUserId),
            FileCtx2
    end.

%%--------------------------------------------------------------------
%% @doc
%% Chown specific file according to given UserId and SpaceId
%% @end
%%--------------------------------------------------------------------
-spec chown_file(file_ctx:ctx()) -> file_ctx:ctx().
chown_file(FileCtx) ->
    {SFMHandle, FileCtx2} = storage_file_manager:new_handle(?ROOT_SESS_ID, FileCtx),
    {#document{value = #file_meta{owner = OwnerUserId}}, FileCtx3} =
        file_ctx:get_file_doc(FileCtx2),
    SpaceId = file_ctx:get_space_id_const(FileCtx3),
    (catch storage_file_manager:chown(SFMHandle, OwnerUserId, SpaceId)), %todo implement chown in s3/ceph and remove this catch
    FileCtx3.

%%--------------------------------------------------------------------
%% @doc
%% Saves permission cache.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates permission cache.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, id()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, id()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Returns permission cache.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes permission cache.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether permission cache exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(id()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add file that need to be chowned in future.
%% @end
%%--------------------------------------------------------------------
-spec add(file_ctx:ctx(), od_user:id()) -> {ok, datastore:id()} | {error, term()}.
add(FileCtx, UserId) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    UpdateFun = fun(Val = #files_to_chown{file_guids = Guids}) ->
        case lists:member(FileGuid, Guids) of
            true ->
                {ok, Val};
            false ->
                {ok, Val#files_to_chown{file_guids = [FileGuid | Guids]}}
        end
    end,
    DocToCreate = #document{key = UserId, value = #files_to_chown{
        file_guids = [FileGuid]
    }},
    create_or_update(DocToCreate, UpdateFun).

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