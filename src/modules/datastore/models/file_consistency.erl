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
-export([wait/4, add_components_and_notify/2, check_and_add_components/3,
    check_missing_components/2, check_missing_components/3]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
model_init/0, 'after'/5, before/4]).
-export([record_struct/1]).

-type id() :: file_meta:id().
-type component() :: file_meta | local_file_location | parent_links | link_to_parent | custom_metadata | times
    | {link_to_child, file_meta:name(), file_meta:uuid()} | {rev, Module :: atom(), Rev :: non_neg_integer()}.
-type waiting() :: {[component()], pid(), PosthookArguments :: list()}.
-type restart_posthook() :: Args:: list() | {Module :: atom(), Function:: atom(), Args:: list()}.

-export_type([id/0, component/0, waiting/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {components_present, [term]},
        {waiting, [{[term], term, term}]}
    ]}.

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
-spec wait(file_meta:uuid(), od_space:id(), [file_consistency:component()], restart_posthook()) -> ok.
wait(FileUuid, SpaceId, WaitFor, RestartPosthookData) ->
    {NeedsToWait, WaitForParent} = critical_section:run([?MODEL_NAME, <<"consistency_", FileUuid/binary>>],
        fun() ->
            Doc = case get(FileUuid) of
                {ok, D = #document{value = #file_consistency{}}} ->
                    D;
                {error, {not_found, file_consistency}} ->
                    #document{key = FileUuid, value = #file_consistency{}}
            end,
            #document{value = FC = #file_consistency{components_present = ComponentsPresent, waiting = Waiting}} = Doc,
            MissingComponents = WaitFor -- ComponentsPresent,
            FoundComponents = check_missing_components(FileUuid, SpaceId, MissingComponents),
            UpdatedMissingComponents = MissingComponents -- FoundComponents,
            {UpdatedPresentComponents0, ParentLinksPartial} = lists:foldl(fun(FComp, {Acc1, Acc2}) ->
                case FComp of
                    {parent_links_partial, Part} ->
                        {Acc1, [Part | Acc2]};
                    _ ->
                        {[FComp | Acc1], Acc2}
                end
            end, {ComponentsPresent, []}, FoundComponents),
            UpdatedPresentComponents = lists:usort(UpdatedPresentComponents0),

            case UpdatedMissingComponents of
                []->
                    NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents}}),
                    {ok, _} = save(NewDoc),
                    {false, ParentLinksPartial};
                [parent_links] ->
                    NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents}}),
                    {ok, _} = save(NewDoc),
                    {false, [parent_links | ParentLinksPartial]};
                _ ->
                    NewDoc = notify_waiting(Doc#document{value = FC#file_consistency{components_present = UpdatedPresentComponents,
                        waiting = [{UpdatedMissingComponents -- [parent_links], self(), RestartPosthookData} | Waiting]}}),
                    {ok, _} = save(NewDoc),
                    {true, case lists:member(parent_links, UpdatedMissingComponents) of
                               true -> [parent_links | ParentLinksPartial];
                               _ -> ParentLinksPartial
                           end}
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
    case WaitForParent of
        [] ->
            ok;
        [parent_links] ->
            {ok, ParentUuid} = file_meta:get_parent_uuid(FileUuid, SpaceId),
            file_consistency:wait(ParentUuid, SpaceId, [file_meta, times, link_to_parent, parent_links],
                RestartPosthookData);
        [parent_links, {NewUuid, WaitName, ChildUuid}] ->
            file_consistency:wait(NewUuid, SpaceId, [file_meta, times, link_to_parent, parent_links,
                {link_to_child, WaitName, ChildUuid}], RestartPosthookData)
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
    critical_section:run([?MODEL_NAME, <<"consistency_", FileUuid/binary>>],
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
notify_waiting(Doc = #document{key = FileUuid,
    value = FC = #file_consistency{components_present = Components, waiting = Waiting}}) ->
    NewWaiting = lists:filter(fun({Missing, Pid, RestartPosthookData}) ->
        case Missing -- Components of
            [] ->
                notify_pid(Pid, RestartPosthookData),
                false;
            [{link_to_child, WaitName, ChildUuid}] ->
                case catch file_meta:get_child({uuid, FileUuid}, WaitName) of
                    {ok, ChildrenUUIDs} ->
                        case lists:member(ChildUuid, ChildrenUUIDs) of
                            true ->
                                notify_pid(Pid, RestartPosthookData),
                                false;
                            false ->
                                true
                        end;
                    _ ->
                        true
                end;
            [{rev, Model, Rev}] ->
                case verify_revision(FileUuid, Model, Rev) of
                    ok ->
                        notify_pid(Pid, RestartPosthookData),
                        false;
                    _ ->
                        true
                end;
            _ ->
                true
        end
    end, Waiting),
    Doc#document{value = FC#file_consistency{waiting = NewWaiting}}.

%%--------------------------------------------------------------------
%% @doc
%% Check if components are present, add them to file_consistency and
%% notify waiting processes.
%% @end
%%--------------------------------------------------------------------
-spec check_and_add_components(file_meta:uuid(), od_space:id(), [component()]) -> ok.
check_and_add_components(FileUuid, SpaceId, Components) ->
    FoundComponents = check_missing_components(FileUuid, SpaceId, Components),
    add_components_and_notify(FileUuid, FoundComponents).

%%--------------------------------------------------------------------
%% @doc
%% Checks if components of file are present.
%% @end
%%--------------------------------------------------------------------
-spec check_missing_components(file_meta:uuid(), od_space:id()) ->
    [component() | {parent_links_partial, {file_meta:uuid(), file_meta:name(), file_meta:uuid()}}].
check_missing_components(FileUuid, SpaceId) ->
    check_missing_components(FileUuid, SpaceId, [file_meta, local_file_location, link_to_parent, parent_links]).

%%--------------------------------------------------------------------
%% @doc
%% Checks if components of file are present.
%% @end
%%--------------------------------------------------------------------
-spec check_missing_components(file_meta:uuid(), od_space:id(), [component()]) ->
    [component() | {parent_links_partial, {file_meta:uuid(), file_meta:name(), file_meta:uuid()}}].
check_missing_components(FileUuid, SpaceId, Missing) ->
    check_missing_components(FileUuid, SpaceId, Missing, []).

%%--------------------------------------------------------------------
%% @doc
%% Checks if components of file are present.
%% @end
%%--------------------------------------------------------------------
-spec check_missing_components(file_meta:uuid(), od_space:id(), [component()], [component()]) ->
    [component() | {parent_links_partial, {file_meta:uuid(), file_meta:name(), file_meta:uuid()}}].
check_missing_components(_FileUuid, _SpaceId, [], Found) ->
    Found;
check_missing_components(FileUuid, SpaceId, [file_meta | RestMissing], Found) ->
    case catch file_meta:get(FileUuid) of
        {ok, _} ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [file_meta | Found]);
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end;
check_missing_components(FileUuid, SpaceId, [times | RestMissing], Found) ->
    case catch times:get(FileUuid) of
        {ok, _} ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [times | Found]);
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end;
check_missing_components(FileUuid, SpaceId, [local_file_location | RestMissing], Found) ->
    case catch fslogic_utils:get_local_file_location({uuid, FileUuid}) of
        #document{} ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [local_file_location | Found]);
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end;
check_missing_components(FileUuid, SpaceId, [parent_links | RestMissing], Found) ->
    case catch fslogic_path:check_path(FileUuid) of
        ok ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [parent_links | Found]);
        path_beg_error ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found);
        {path_error, NewArgs} ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [{parent_links_partial, NewArgs} | Found])
    end;
check_missing_components(FileUuid, SpaceId, [{link_to_child, WaitName, ChildUuid} | RestMissing], Found) ->
    case catch file_meta:get_child({uuid, FileUuid}, WaitName)  of
        {ok, ChildrenUUIDs} ->
            case lists:member(ChildUuid, ChildrenUUIDs) of
                true ->
                    check_missing_components(FileUuid, SpaceId, RestMissing, [{link_to_child, WaitName, ChildUuid} | Found]);
                false ->
                    check_missing_components(FileUuid, SpaceId, RestMissing, Found)
            end;
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end;
% Checking local links may be useful in future
%%check_missing_components(FileUuid, SpaceId, [links | RestMissing], Found) ->
%%    case catch file_meta:exists_local_link_doc(FileUuid) of
%%        true ->
%%            check_missing_components(FileUuid, SpaceId, RestMissing, [links | Found]);
%%        _ ->
%%            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
%%    end;
check_missing_components(FileUuid, SpaceId, [link_to_parent | RestMissing], Found) ->
    case catch file_meta:get_parent_uuid(FileUuid, SpaceId) of
        {ok, _} ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [link_to_parent | Found]);
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end;
check_missing_components(FileUuid, SpaceId, [{rev, Model, Rev} | RestMissing], Found) ->
    case verify_revision(FileUuid, Model, Rev) of
        ok ->
            check_missing_components(FileUuid, SpaceId, RestMissing, [{rev, Model, Rev} | Found]);
        _ ->
            check_missing_components(FileUuid, SpaceId, RestMissing, Found)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if revision of document is present.
%% @end
%%--------------------------------------------------------------------
-spec verify_revision(file_meta:uuid(), atom(), non_neg_integer()) ->
    ok | younger_revision | not_found.
verify_revision(FileUuid, Model, Rev) ->
    Driver = datastore:driver_to_module(datastore:level_to_driver(?DISK_ONLY_LEVEL)),
    MC = Model:model_init(),
    % TODO - what if document has been deleted in parallel by other provider?
    % TODO - get all revisions simmilar to process_raw_doc
    case catch erlang:apply(Driver, get, [MC, FileUuid]) of
        {ok, #document{rev = CurrentRev}} ->
            case couchdb_datastore_driver:rev_to_number(CurrentRev) >= Rev of
                true ->
                    ok;
                _ ->
                    younger_revision
            end;
        _ ->
            not_found
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
    % TODO - clear file consistency info when file is ok (triggered with time)
    ?MODEL_CONFIG(file_consistency_bucket, [{file_meta, delete}], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
% TODO - check if it is not done too fast (VFS-2411)
'after'(file_meta, delete, ?GLOBAL_ONLY_LEVEL, [Key, _], ok) ->
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notifies waiting process that file is consistent.
%% @end
%%--------------------------------------------------------------------
-spec notify_pid(pid(), restart_posthook()) -> ok.
notify_pid(Pid, RestartPosthookData) ->
    Pid ! file_is_now_consistent,
    case is_process_alive(Pid) of
        true ->
            ok;
        _ ->
            case RestartPosthookData of
                {M, F, Args} ->
                    spawn(M, F, Args);
                _ ->
                    spawn(dbsync_events, change_replicated, RestartPosthookData)
            end
    end,
    ok.