%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc DBSync hooks.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_events).
-author("Rafal Slota").

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([change_replicated/2, links_changed/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for change_replicated_internal, ignoring unsupported spaces.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated(SpaceId :: binary(), dbsync_worker:change()) ->
    any().
change_replicated(SpaceId, Change) ->
    case is_supported(SpaceId) of
        true ->
            change_replicated_internal(SpaceId, Change);
        false ->
            ?warning("Change of unsupported space ~p received", [SpaceId]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Hook that runs just after change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated_internal(SpaceId :: binary(), dbsync_worker:change()) ->
    any() | no_return().
change_replicated_internal(_SpaceId, #change{model = file_meta, doc =  #document{key = FileUuid,
    value = #file_meta{type = ?REGULAR_FILE_TYPE}, deleted = true}}) ->
    ok = replica_cleanup:clean_replica_files(FileUuid),
    file_consistency:delete(FileUuid);
change_replicated_internal(SpaceId, Change = #change{model = file_meta, doc = FileDoc =
    #document{key = FileUuid, value = #file_meta{type = ?REGULAR_FILE_TYPE}}}) ->
    ?debug("change_replicated_internal: changed file_meta ~p", [FileUuid]),
    ok = file_consistency:wait(FileUuid, SpaceId, [times, link_to_parent, parent_links], [SpaceId, Change]),
    ok = sfm_utils:create_storage_file_if_not_exists(SpaceId, FileDoc),
    ok = fslogic_event:emit_file_attr_changed({uuid, FileUuid}, []),
    ok = file_consistency:add_components_and_notify(FileUuid, [file_meta, local_file_location]),
    ok = file_consistency:check_and_add_components(FileUuid, SpaceId, [parent_links]);
change_replicated_internal(SpaceId, Change = #change{model = file_meta, doc = #document{key = FileUuid, value = #file_meta{}}}) ->
    ?debug("change_replicated_internal: changed file_meta ~p", [FileUuid]),
    ok = file_consistency:wait(FileUuid, SpaceId, [times], [SpaceId, Change]),
    ok = fslogic_event:emit_file_attr_changed({uuid, FileUuid}, []),
    ok = file_consistency:add_components_and_notify(FileUuid, [file_meta]),
    ok = file_consistency:check_and_add_components(FileUuid, SpaceId, [parent_links]);
change_replicated_internal(SpaceId, Change = #change{model = file_location, doc = Doc = #document{value = #file_location{uuid = FileUuid}}}) ->
    ?debug("change_replicated_internal: changed file_location ~p", [FileUuid]),
    ok = file_consistency:wait(FileUuid, SpaceId, [file_meta, times, local_file_location], [SpaceId, Change]),
    ok = replica_dbsync_hook:on_file_location_change(SpaceId, Doc);
change_replicated_internal(SpaceId, #change{model = file_meta, doc = #document{value = #links{model = file_meta, doc_key = FileUuid}}}) ->
    ?debug("change_replicated_internal: changed links ~p", [FileUuid]),
    ok = file_consistency:check_and_add_components(FileUuid, SpaceId, [link_to_parent, parent_links]);
change_replicated_internal(_SpaceId, #change{model = times, doc = #document{key = FileUuid, value = #times{}}}) ->
    ?debug("change_replicated_internal: changed times ~p", [FileUuid]),
    ok = file_consistency:add_components_and_notify(FileUuid, [times]);
change_replicated_internal(SpaceId, #change{model = change_propagation_controller,
    doc = #document{deleted = false, value = #links{model = change_propagation_controller, doc_key = DocKey}}}) ->
    ?debug("change_replicated_internal: change_propagation_controller links ~p", [DocKey]),
    {ok, _} = change_propagation_controller:verify_propagation(DocKey, SpaceId, false);
change_replicated_internal(_SpaceId, #change{model = change_propagation_controller,
    doc = #document{deleted = false, key = Key} = Doc}) ->
    ?debug("change_replicated_internal: change_propagation_controller ~p", [Key]),
    ok = change_propagation_controller:mark_change_propagated(Doc);
change_replicated_internal(_SpaceId, #change{model = custom_metadata,
    doc = #document{key = FileUuid, value = #custom_metadata{}}}) ->
    ?debug("change_replicated_internal: changed custom_metadata ~p", [FileUuid]),
    ok = file_consistency:add_components_and_notify(FileUuid, [custom_metadata]);
change_replicated_internal(_SpaceId, _Change) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Hook that runs while link change is replicated from remote provider to apply it to local link trees.
%% Important - providers' IDs must be used as scope IDs
%% @end
%%--------------------------------------------------------------------
-spec links_changed(Origin :: links_utils:scope(), ModelName :: model_behaviour:model_type(),
    MainDocKey :: datastore:ext_key(), AddedMap :: #{}, DeletedMap :: #{}) ->
    ok.
links_changed(_Origin, ModelName, MainDocKey, AddedMap, DeletedMap) ->
    MC0 = #model_config{link_store_level = _LinkStoreLevel} = ModelName:model_init(),
    MC = MC0#model_config{disable_remote_link_delete = true},
    MyProvID = oneprovider:get_provider_id(),

    maps:fold(
        fun(K, {Version, Targets}, AccIn) ->
            NewTargets = lists:filter(
                fun
                    ({_, {deleted, _}, _, _}) -> %% Get all links that are marked as deleted
                        true;
                    ({Scope, _, _, _}) ->
                        Scope =/= MyProvID
                end, Targets),
            case NewTargets of
                [] -> AccIn;
                _ ->
                    {NewTargetsAdd, NewTargetsDel} = lists:partition(fun
                        ({_, {deleted, _}, _, _}) ->
                            false;
                        (_) ->
                            true
                        end, NewTargets),
                    case NewTargetsAdd of
                        [] -> ok;
                        _ ->
                            MC1 = MC#model_config{link_replica_scope = ?DEFAULT_LINK_REPLICA_SCOPE},
                            ok = datastore:add_links(?DISK_ONLY_LEVEL, MainDocKey, MC1, [{K, {Version, NewTargetsAdd}}])
                    end,

                    %% Handle links marked as deleted
                    lists:foreach(
                        fun({Scope0, {deleted, VH0}, _, _}) ->
                            ok = datastore:delete_links(?DISK_ONLY_LEVEL, MainDocKey, MC,
                                [links_utils:make_scoped_link_name(K, Scope0, VH0, size(Scope0))])
                        end, NewTargetsDel)

            end
        end, #{}, AddedMap),

    maps:fold(
        fun(K, V, _AccIn) ->
            {_, DelTargets} = V,
            lists:foreach(
                fun({S, VH0, _, _}) ->
                    case VH0 of
                        {deleted, VH1} ->
                            ok; %% Ignore deletion of deleted link
                        VH1 ->
                            ok = datastore:delete_links(?DISK_ONLY_LEVEL, MainDocKey, MC, [links_utils:make_scoped_link_name(K, S, VH1, size(S))])
                    end
                end, DelTargets)
        end, [], DeletedMap),

    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if space is supported by current provider.
%% @end
%%--------------------------------------------------------------------
-spec is_supported(SpaceId :: binary()) -> boolean().
is_supported(SpaceId) ->
    lists:member(oneprovider:get_provider_id(), dbsync_utils:get_providers_for_space(SpaceId)).