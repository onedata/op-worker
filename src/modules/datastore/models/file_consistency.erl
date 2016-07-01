%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that keeps track of consistency of file metadata
%%% @end
%%%-------------------------------------------------------------------
-module(file_consistency).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([wait/2, update_consistency/2, add_components_and_notify/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
model_init/0, 'after'/5, before/4]).


-export_type([id/0]).

-type id() :: file_meta:id().
-type component() :: file_meta | local_file_location | parent_links | links.


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wait for file metadata to become consistent
%% @end
%%--------------------------------------------------------------------
-spec wait(file_meta:uuid(), list()) -> ok.
wait(FileUuid, WaitFor) ->
    MissingComponents = datastore:run_synchronized(?MODEL_NAME, <<"consistency_", FileUuid/binary>>,
        fun() ->
            case get(FileUuid) of
                {ok, Doc = #document{value = FC = #file_consistency{components_present = ComponentsPresent, waiting = Waiting}}} ->
                    case WaitFor -- ComponentsPresent of
                        [] ->
                            NewDoc = notify_waiting(Doc),
                            case NewDoc == Doc of
                                true ->
                                    [];
                                false ->
                                    {ok, _} = save(NewDoc),
                                    []
                            end;
                        MissingComponents ->
                            NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{waiting = [{MissingComponents, self()} | Waiting]}}),
                            {ok, _} = save(NewDoc),
                            MissingComponents
                    end;
                {error, {not_found, file_consistency}} ->
                    {ok, _} = create(#document{key = FileUuid, value = #file_consistency{waiting = [{WaitFor, self()}]}}),
                    WaitFor
            end
        end),

    FoundComponents = check_missing_components(FileUuid, MissingComponents, []),
    add_components_and_notify(FileUuid, FoundComponents),
    case MissingComponents -- FoundComponents of
        [] ->
            ok;
        _ ->
            receive
                file_is_now_consistent ->
                    ok
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks and update consistency info
%% @end
%%--------------------------------------------------------------------
-spec update_consistency(file_meta:uuid(), [component()]) -> ok.
update_consistency(FileUuid, ComponentsToUpdate) ->
    FoundComponents = check_missing_components(FileUuid, ComponentsToUpdate, []),
    add_components_and_notify(FileUuid, FoundComponents).

%%--------------------------------------------------------------------
%% @doc
%% Add found components to file consistency models
%% @end
%%--------------------------------------------------------------------
-spec add_components_and_notify(file_meta:uuid(), [component()]) -> ok.
add_components_and_notify(_FileUuid, []) ->
    ok;
add_components_and_notify(FileUuid, FoundComponents) ->
    datastore:run_synchronized(?MODEL_NAME, <<"consistency_", FileUuid/binary>>,
        fun() ->
            case get(FileUuid) of
                {ok, Doc = #document{value = FC = #file_consistency{
                    components_present = ComponentsPresent, waiting = Waiting}}} ->
                    UpdatedComponents = lists:usort(ComponentsPresent ++ FoundComponents),

                    NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present =
                            UpdatedComponents, waiting = Waiting}}),

                    case NewDoc =/= Doc of
                            true ->
                                {ok, _} = save(NewDoc),
                                ok;
                            false ->
                                ok
                        end;
                {error, {not_found, file_consistency}} ->
                    ok
            end
        end).

notify_waiting(Doc = #document{value = FC = #file_consistency{components_present = Components, waiting = Waiting}}) ->
    NewWaiting = lists:filter(fun({Missing, Pid}) ->
        case Missing -- Components of
            [] ->
                Pid ! file_is_now_consistent,
                false;
            _ ->
                true
        end
    end, Waiting),
    Doc#document{value = FC#file_consistency{waiting = NewWaiting}}.

%%--------------------------------------------------------------------
%% @doc
%% Checks if components of file are present.
%% @end
%%--------------------------------------------------------------------
-spec check_missing_components(file_meta:uuid(), [component()], [component()]) -> [component()].
check_missing_components(_FileUuid, [], Found) ->
    Found;
check_missing_components(FileUuid, [file_meta | RestMissing], Found) ->
    case catch file_meta:get(FileUuid) of
        {ok, _} ->
            check_missing_components(FileUuid, RestMissing, [file_meta | Found]);
        _ ->
            check_missing_components(FileUuid, RestMissing, Found)
    end;
check_missing_components(FileUuid, [local_file_location | RestMissing], Found) ->
    case catch fslogic_utils:get_local_file_location({uuid, FileUuid}) of
        #document{} ->
            check_missing_components(FileUuid, RestMissing, [local_file_location | Found]);
        _ ->
            check_missing_components(FileUuid, RestMissing, Found)
    end;
check_missing_components(FileUuid, [parent_links | RestMissing], Found) ->
    case catch fslogic_path:gen_path({uuid, FileUuid}, ?ROOT_SESS_ID) of
        {ok, _} ->
            check_missing_components(FileUuid, RestMissing, [parent_links | Found]);
        _ ->
            check_missing_components(FileUuid, RestMissing, Found)
    end;
check_missing_components(FileUuid, [links | RestMissing], Found) ->
    ProviderId = oneprovider:get_provider_id(),
    case catch file_meta:exists({uuid, links_utils:links_doc_key(FileUuid, ProviderId)}) of
        true ->
            check_missing_components(FileUuid, RestMissing, [links | Found]);
        _ ->
            check_missing_components(FileUuid, RestMissing, Found)
    end.

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
    ?MODEL_CONFIG(file_consistency_bucket, [], ?DISK_ONLY_LEVEL).

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
