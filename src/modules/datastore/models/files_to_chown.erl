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
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add/2, chown_file/3]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

-export_type([id/0]).

-type id() :: onedata_user:id().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add file that need to be chowned in future.
%% @end
%%--------------------------------------------------------------------
-spec add(onedata_user:id(), file_meta:uuid()) -> {ok, datastore:key()} | datastore:generic_error().
add(UserId, FileUuid) ->
    %todo add create_or_update operation to datastore
    UpdateFun = fun(Val = #files_to_chown{file_uuids = Uuids}) ->
        {ok, Val#files_to_chown{file_uuids = lists:usort([FileUuid | Uuids])}}
    end,
    case update(UserId, UpdateFun) of
        {ok, Key} ->
            {ok, Key};
        {error, {not_found, files_to_chown}} ->
            create(#document{key = UserId, value = #files_to_chown{file_uuids = [FileUuid]}});
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Chown specific file according to given UserId and SpaceId
%% @end
%%--------------------------------------------------------------------
-spec chown_file(file_meta:uuid(), onedata_user:id(), space_info:id()) -> ok.
chown_file(FileUuid, UserId, SpaceId) ->
    LocalLocations = fslogic_utils:get_local_storage_file_locations({uuid, FileUuid}),
    lists:foreach(fun({StorageId, FileId}) ->
        try
            SpaceUUID = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
            {ok, Storage} = storage:get(StorageId),
            SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceUUID, FileUuid, Storage, FileId),

            ok = storage_file_manager:chown(SFMHandle, UserId, SpaceId)
        catch
            _:Error ->
                ?error_stacktrace("Cannot chown file ~p, due to error ~p", [FileUuid, Error])
        end
    end, LocalLocations).

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
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(files_to_chown_bucket, [{onedata_user, create}, {onedata_user, save},
        {onedata_user, create_or_update}], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(onedata_user, create, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    chown_pending_files(UUID);
'after'(onedata_user, save, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    chown_pending_files(UUID);
'after'(onedata_user, create_or_update, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    chown_pending_files(UUID);
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

%%--------------------------------------------------------------------
%% @doc
%% Chown all pending files of given user
%% @end
%%--------------------------------------------------------------------
-spec chown_pending_files(onedata_user:id()) -> ok.
chown_pending_files(UserId) ->
    case files_to_chown:get(UserId) of
        {ok, #document{value = #files_to_chown{file_uuids = FileUuids}}} ->
            lists:foreach(fun chown_pending_file/1, FileUuids),
            delete(UserId);
        {error,{not_found,files_to_chown}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Chown given file to its owner
%% @end
%%--------------------------------------------------------------------
-spec chown_pending_file(file_meta:uuid()) -> ok.
chown_pending_file(FileUuid) ->
    try
        {ok, #document{value = #file_meta{uid = UserId}}} = file_meta:get({uuid, FileUuid}),
        {ok, #document{key = SpaceDirUuid}} = file_meta:get_scope({uuid, FileUuid}),
        SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceDirUuid),
        chown_file(FileUuid, UserId, SpaceId)
    catch
        _:Error ->
            ?error_stacktrace("Cannot chown pending file ~p due to error ~p", [FileUuid, Error])
    end.


