%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that stores open files.
%%% @end
%%%-------------------------------------------------------------------
-module(file_handles).
-author("Michal Wrona").
-author("Krzysztof Trzepla").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([register_open/3, register_release/3, mark_to_remove/1,
    invalidate_session_entry/2]).

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).
-export([record_struct/1, record_upgrade/2]).


%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {is_removed, boolean},
        {active_descriptors, #{string => integer}}
    ]};
record_struct(2) ->
    {record, Struct} = record_struct(1),
    {record, lists:keyreplace(
        active_descriptors, 1, Struct, {descriptors, #{string => integer}}
    )}.


%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, IsRemoved, Descriptors}) ->
    {2, #file_handles{is_removed = IsRemoved, descriptors = Descriptors}}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(open_file_bucket, [], ?GLOBALLY_CACHED_LEVEL),
    Config#model_config{version = 2}.

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
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers number given in Count of new file descriptors for given
%% FileUuid and SessionId.
%% @end
%%--------------------------------------------------------------------
-spec register_open(file_meta:uuid(), session:id(), pos_integer()) ->
    ok | {error, Reason :: term()}.
register_open(_, ?ROOT_SESS_ID, _) ->
    ok;
register_open(_, ?GUEST_SESS_ID, _) ->
    ok;
register_open(FileUuid, SessId, Count) ->
    Diff = fun
        (#file_handles{is_removed = true}) ->
            {error, phantom_file};
        (#file_handles{descriptors = Fds} = Handle) ->
            case maps:get(SessId, Fds, 0) of
                0 -> case session:add_open_file(SessId, FileUuid) of
                    ok -> {ok, Handle#file_handles{
                        descriptors = maps:put(SessId, Count, Fds)
                    }};
                    {error, Reason} -> {error, Reason}
                end;
                FdCount -> {ok, Handle#file_handles{
                    descriptors = maps:put(SessId, FdCount + Count, Fds)
                }}
            end
    end,

    case update(FileUuid, Diff) of
        {ok, _} -> ok;
        {error, {not_found, _}} ->
            Doc = #document{key = FileUuid, value = #file_handles{
                descriptors = #{SessId => Count}
            }},
            case create(Doc) of
                {ok, _} -> session:add_open_file(SessId, FileUuid);
                {error, already_exists} ->
                    register_open(FileUuid, SessId, Count);
                {error, Reason} -> {error, Reason}
            end;
        {error, phantom_file} -> {error, {not_found, ?MODEL_NAME}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes number given in Count of file descriptors for given
%% FileUuid and SessionId. Removes file if no file descriptor
%% is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec register_release(file_meta:uuid(), session:id(), pos_integer() | infinity) ->
    ok | {error, Reason :: term()}.
register_release(_, ?ROOT_SESS_ID, _) ->
    ok;
register_release(_, ?GUEST_SESS_ID, _) ->
    ok;
register_release(FileUuid, SessId, Count) ->
    Diff = fun(#file_handles{is_removed = Removed, descriptors = Fds} = Handle) ->
        FdCount = maps:get(SessId, Fds, 0),
        case Count =:= infinity orelse FdCount =< Count of
            true -> case session:remove_open_file(SessId, FileUuid) of
                ok ->
                    Fds2 = maps:remove(SessId, Fds),
                    case {Removed, maps:size(Fds2)} of
                        {true, 0} -> {error, phantom_file};
                        _ -> {ok, Handle#file_handles{descriptors = Fds2}}
                    end;
                {error, Reason} -> {error, Reason}
            end;
            false -> {ok, Handle#file_handles{
                descriptors = maps:put(SessId, FdCount - Count, Fds)
            }}
        end
    end,

    case update(FileUuid, Diff) of
        {ok, _} -> maybe_delete(FileUuid);
        {error, phantom_file} ->
            FileGuid = %todo VFS-3017 check why we cannot get space_id for onedata hidden files
                try fslogic_uuid:uuid_to_guid(FileUuid)
                catch _:_ -> fslogic_uuid:uuid_to_guid(FileUuid, undefined)
                end,
            FileCtx = file_ctx:new_by_guid(FileGuid),
            fslogic_deletion_worker:request_open_file_deletion(FileCtx),
            delete(FileUuid);
        {error, {not_found, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks files as removed.
%% @end
%%--------------------------------------------------------------------
-spec mark_to_remove(file_meta:uuid()) -> ok | {error, Reason :: term()}.
mark_to_remove(FileUuid) ->
    case update(FileUuid, #{is_removed => true}) of
        {ok, _} -> ok;
        {error, {not_found, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Clears descriptors count associated with SessionId for given FileUuid.
%% Removes file if no file descriptor is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_session_entry(file_meta:uuid(), session:id()) ->
    ok | {error, Reason :: term()}.
invalidate_session_entry(FileUuid, SessId) ->
    case register_release(FileUuid, SessId, infinity) of
        ok -> ok;
        {error, {not_found, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file handles if descriptors map is empty.
%% @end
%%--------------------------------------------------------------------
-spec maybe_delete(FileUuid :: file_meta:uuid()) -> ok | {error, Reason :: term()}.
maybe_delete(FileUuid) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, FileUuid, fun() ->
        case file_handles:get(FileUuid) of
            {ok, #document{value = #file_handles{descriptors = Fds}}} ->
                maps:size(Fds) == 0;
            {error, _} -> false
        end
    end).
