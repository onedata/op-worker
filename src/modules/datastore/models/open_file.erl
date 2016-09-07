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
-module(open_file).
-author("Michal Wrona").
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
    ?MODEL_CONFIG(open_file_bucket, [], ?GLOBALLY_CACHED_LEVEL).

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

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure
%% that 2 funs with same ResourceId won't run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_in_critical_section(ResourceId :: binary(), Fun :: fun(() -> Result :: term())) -> Result :: term().
run_in_critical_section(ResourceId, Fun) ->
    critical_section:run([?MODEL_NAME, ResourceId], Fun).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers number given in Count of new file descriptors for given
%% FileUUID and SessionId.
%% @end
%%--------------------------------------------------------------------
-spec register_open(file_meta:uuid(), session:id(), non_neg_integer()) ->
    ok | {error, Reason :: term()}.
register_open(FileUUID, SessionId, Count) ->
    run_in_critical_section(FileUUID, fun() ->
        case open_file:get(FileUUID) of
            {ok, #document{value = OpenFile} = Doc} ->
                #open_file{active_descriptors = ActiveDescriptors} = OpenFile,

                ActiveForSession = maps:get(SessionId, ActiveDescriptors, 0),
                case ActiveForSession of
                    0 ->
                        ok = session:add_open_file(SessionId, FileUUID);
                    _ -> ok
                end,
                UpdatedActiveDescriptors = maps:put(SessionId,
                    ActiveForSession + Count, ActiveDescriptors),

                {ok, _} = open_file:save(Doc#document{value = OpenFile#open_file{
                    active_descriptors = UpdatedActiveDescriptors}}),
                ok;
            {error, {not_found, _}} ->
                Doc = #document{key = FileUUID, value = #open_file{
                    active_descriptors = #{SessionId => Count}}},

                case open_file:create(Doc) of
                    {ok, _} ->
                        ok = session:add_open_file(SessionId, FileUUID);
                    {error, Reason} -> {error, Reason}
                end;
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Removes number given in Count of file descriptors for given
%% FileUUID and SessionId. Removes file if no file descriptor
%% is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec register_release(file_meta:uuid(), session:id(), non_neg_integer()) ->
    ok | {error, Reason :: term()}.
register_release(FileUUID, SessionId, Count) ->
    run_in_critical_section(FileUUID, fun() ->
        case open_file:get(FileUUID) of
            {ok, #document{value = #open_file{active_descriptors = ActiveDescriptors} = OpenFile} = Doc} ->

                UpdatedActiveDescriptors = case maps:get(SessionId, ActiveDescriptors, 1) of
                    ActiveForSession when ActiveForSession =< Count ->
                        ok = session:remove_open_file(SessionId, FileUUID),
                        maps:remove(SessionId, ActiveDescriptors);
                    ActiveForSession ->
                        maps:put(SessionId, ActiveForSession - Count, ActiveDescriptors)
                end,

                delete_or_save(FileUUID, UpdatedActiveDescriptors, OpenFile, Doc);
            {error, {not_found, _}} ->
                ok;
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Marks files as removed.
%% @end
%%--------------------------------------------------------------------
-spec mark_to_remove(file_meta:uuid()) -> ok | {error, Reason :: term()}.
mark_to_remove(FileUUID) ->
    run_in_critical_section(FileUUID, fun() ->
        case open_file:get(FileUUID) of
            {ok, #document{value = OpenFile} = Doc} ->
                {ok, _} = open_file:save(Doc#document{value = OpenFile#open_file{
                    is_removed = true}}),
                ok;
            {error, Reason} ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Clears descriptors count associated with SessionId for given FileUUID.
%% Removes file if no file descriptor is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_session_entry(file_meta:uuid(), session:id()) ->
    ok | {error, Reason :: term()}.
invalidate_session_entry(FileUUID, SessionId) ->
    run_in_critical_section(FileUUID, fun() ->
        case open_file:get(FileUUID) of
            {ok, #document{value = #open_file{active_descriptors = ActiveDescriptors} = OpenFile} = Doc} ->
                case maps:is_key(SessionId, ActiveDescriptors) of
                    true ->
                        UpdatedActiveDescriptors = maps:remove(SessionId, ActiveDescriptors),
                        delete_or_save(FileUUID, UpdatedActiveDescriptors, OpenFile, Doc);
                    false -> ok
                end;

            {error, {not_found, _}} ->
                ok;
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes entry and file if needed or saves updated state.
%% @end
%%--------------------------------------------------------------------
-spec delete_or_save(file_meta:uuid(), #{session:id() => non_neg_integer()},
    #open_file{}, #document{}) -> ok.
delete_or_save(FileUUID, UpdatedActiveDescriptors, OpenFile, Doc) ->
    case maps:size(UpdatedActiveDescriptors) of
        0 ->
            case OpenFile#open_file.is_removed of
                true ->
                    worker_proxy:cast(file_deletion_worker,
                        {open_file_deletion_request, FileUUID});
                false -> ok
            end,

            ok = open_file:delete(FileUUID);
        _ ->
            {ok, _} = open_file:save(Doc#document{value = OpenFile#open_file{
                active_descriptors = UpdatedActiveDescriptors}}),
            ok
    end.