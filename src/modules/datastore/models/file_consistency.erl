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
-export([wait/3, add_components_and_notify/2]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
model_init/0, 'after'/5, before/4]).

-type id() :: file_meta:id().
-type component() :: file_meta | local_file_location | parent_links | links.
-type waiting() :: {[component()], pid(), PosthookArguments :: list()}.

-export_type([id/0, component/0, waiting/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wait for file metadata to become consistent, the arguments for
%% dbsync_events:change_replicated call are passed to be able to re trigger
%% the change after system restart
%% @end
%%--------------------------------------------------------------------
-spec wait(file_meta:uuid(), [file_consistency:component()], list()) -> ok.
wait(FileUuid, WaitFor, DbsyncPosthookArguments) ->
    {NeedsToWait, NeedsToWaitForParent} = datastore:run_synchronized(?MODEL_NAME, <<"consistency_", FileUuid/binary>>,
        fun() ->
            case get(FileUuid) of
                {ok, Doc = #document{value = FC = #file_consistency{components_present = ComponentsPresent, waiting = Waiting}}} ->
                    MissingComponents = WaitFor -- ComponentsPresent,
                    FoundComponents = check_missing_components(FileUuid, MissingComponents, []),
                    UpdatedMissingComponents = MissingComponents -- FoundComponents,
                    UpdatedPresentComponents = lists:usort(ComponentsPresent ++ FoundComponents),

                    case UpdatedMissingComponents of
                        []->
                            NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents}}),
                            {ok, _} = save(NewDoc),
                            {false, false};
                        [parent_links] ->
                            NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents}}),
                            {ok, _} = save(NewDoc),
                            {false, true};
                        _ ->
                            NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents, waiting = [{UpdatedMissingComponents -- [parent_links], self(), DbsyncPosthookArguments} | Waiting]}}),
                            {ok, _} = save(NewDoc),
                            {true, lists:member(parent_links, UpdatedMissingComponents)}
                    end;
                {error, {not_found, file_consistency}} ->
                    FoundComponents = check_missing_components(FileUuid, WaitFor, []),
                    UpdatedMissingComponents = WaitFor -- FoundComponents,
                        case UpdatedMissingComponents of
                            [] ->
                                NewDoc = #document{key = FileUuid, value = #file_consistency{components_present = FoundComponents}},
                                {ok, _} = create(NewDoc),
                                {false, false};
                            [parent_links] ->
                                NewDoc = #document{key = FileUuid, value = #file_consistency{components_present = FoundComponents}},
                                {ok, _} = create(NewDoc),
                                {false, true};
                            _ ->
                                NewDoc = #document{key = FileUuid, value = #file_consistency{components_present = FoundComponents, waiting = [{WaitFor -- [parent_links], self(), DbsyncPosthookArguments}]}},
                                {ok, _} = create(NewDoc),
                                {true, lists:member(parent_links, UpdatedMissingComponents)}
                        end
            end
        end),

    case NeedsToWait of
        true ->
            receive
                file_is_now_consistent ->
                    ok
            end;
        false ->
            ok
    end,
    case NeedsToWaitForParent of
        true ->
            {ok, ParentUuid} = file_meta:get_parent({uuid, FileUuid}),
            file_consistency:wait(ParentUuid, [file_meta, links, parent_links], DbsyncPosthookArguments);
        false ->
            ok
    end.

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
                    components_present = ComponentsPresent}}} ->
                    UpdatedComponents = lists:usort(ComponentsPresent ++ FoundComponents),

                    NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present =
                            UpdatedComponents}}),

                    case NewDoc =/= Doc of
                            true ->
                                {ok, _} = save(NewDoc),
                                ok;
                            false ->
                                ok
                        end;
                {error, {not_found, file_consistency}} ->
                    {ok, _} = create(#document{key = FileUuid, value = #file_consistency{components_present = FoundComponents}}),
                    ok
            end
        end).

%%--------------------------------------------------------------------
%% @doc
%% Notify waiting processes and return file_consistency doc with updated
%% waiting list
%% @end
%%--------------------------------------------------------------------
-spec notify_waiting(datastore:document()) -> datastore:document().
notify_waiting(Doc = #document{value = FC = #file_consistency{components_present = Components, waiting = Waiting}}) ->
    NewWaiting = lists:filter(fun({Missing, Pid, DbsyncPosthookArguments}) ->
        case Missing -- Components of
            [] ->
                Pid ! file_is_now_consistent,
                case is_process_alive(Pid) of
                    true ->
                        ok;
                    _ ->
                        spawn(dbsync_events, change_replicated, DbsyncPosthookArguments)
                end,
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
    ?MODEL_CONFIG(file_consistency_bucket, [{file_meta, delete}], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(file_meta, delete, ?DISK_ONLY_LEVEL, [Key, _], ok) ->
    file_consistency:delete(Key);
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
