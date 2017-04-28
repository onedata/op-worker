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
-export([chown_or_schedule_chowning/1, chown_file/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).

-export_type([id/0]).

-type id() :: od_user:id().

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {file_uuids, [string]}
    ]};
record_struct(2) ->
    {record, [
        {file_guids, [string]}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, Uuids}) ->
    Guids = lists:map(fun fslogic_uuid:uuid_to_guid/1, Uuids),
    {2, #files_to_chown{file_guids = Guids}}.

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
    SFMHandle = storage_file_manager:new_handle(?ROOT_SESS_ID, FileCtx),
    {#document{value = #file_meta{owner = OwnerUserId}}, FileCtx4} =
        file_ctx:get_file_doc(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx4),
    (catch storage_file_manager:chown(SFMHandle, OwnerUserId, SpaceId)), %todo implement chown in s3/ceph and remove this catch
    FileCtx4.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(files_to_chown_bucket, [{od_user, create},
        {od_user, save}, {od_user, create_or_update}], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 2}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(od_user, create, _, _, {ok, Uuid}) ->
    chown_pending_files(Uuid);
'after'(od_user, save, _, _, {ok, Uuid}) ->
    chown_pending_files(Uuid);
'after'(od_user, create_or_update, _, _, {ok, Uuid}) ->
    chown_pending_files(Uuid);
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
%% @private
%% @doc
%% Add file that need to be chowned in future.
%% @end
%%--------------------------------------------------------------------
-spec add(file_ctx:ctx(), od_user:id()) -> {ok, datastore:key()} | datastore:generic_error().
add(FileCtx, UserId) ->
    %todo add create_or_update operation to datastore
    FileGuid = file_ctx:get_guid_const(FileCtx),
    UpdateFun = fun(Val = #files_to_chown{file_guids = Guids}) ->
        case lists:member(FileGuid, Guids) of
            true ->
                {ok, Val};
            false ->
                {ok, Val#files_to_chown{file_guids = [FileGuid | Guids]}}
        end
    end,
    case update(UserId, UpdateFun) of
        {ok, Key} ->
            {ok, Key};
        {error, {not_found, files_to_chown}} ->
            create(#document{
                key = UserId,
                value = #files_to_chown{file_guids = [FileGuid]}
            });
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chown all pending files of given user
%% @end
%%--------------------------------------------------------------------
-spec chown_pending_files(od_user:id()) -> ok.
chown_pending_files(UserId) ->
    case files_to_chown:get(UserId) of
        {ok, #document{value = #files_to_chown{file_guids = FileGuids}}} ->
            lists:foreach(fun chown_pending_file/1, FileGuids),
            delete(UserId);
        {error,{not_found,files_to_chown}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chown given file to its owner
%% @end
%%--------------------------------------------------------------------
-spec chown_pending_file(fslogic_worker:file_guid()) -> file_ctx:ctx().
chown_pending_file(FileGuid) ->
    try
        FileCtx = file_ctx:new_by_guid(FileGuid),
        chown_file(FileCtx)
    catch
        _:Error ->
            ?error_stacktrace("Cannot chown pending file ~p due to error ~p", [FileGuid, Error])
    end.