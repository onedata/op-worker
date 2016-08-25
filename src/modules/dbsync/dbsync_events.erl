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
    any().
change_replicated_internal(_SpaceId, #change{model = file_meta, doc =  #document{key = FileUUID,
    value = #file_meta{type = ?REGULAR_FILE_TYPE}, deleted = true}}) ->
    ok = replica_cleanup:clean_replica_files(FileUUID),
    file_consistency:delete(FileUUID);
change_replicated_internal(SpaceId, Change = #change{model = file_meta, doc = FileDoc =
    #document{key = FileUUID, value = #file_meta{type = ?REGULAR_FILE_TYPE}}}) ->
    ?info("change_replicated_internal: changed file_meta ~p", [FileUUID]),
    ok = file_consistency:wait(FileUUID, SpaceId, [file_meta, link_to_parent, parent_links], [SpaceId, Change]),
    ok = fslogic_file_location:create_storage_file_if_not_exists(SpaceId, FileDoc),
    ok = fslogic_event:emit_file_attr_update({uuid, FileUUID}, []),
    ok = file_consistency:add_components_and_notify(FileUUID, [local_file_location]),
    ok = file_consistency:check_and_add_components(FileUUID, SpaceId, [parent_links]);
change_replicated_internal(SpaceId, #change{model = file_meta, doc = #document{key = FileUUID, value = #file_meta{}}}) ->
    ?info("change_replicated_internal: changed file_meta ~p", [FileUUID]),
    ok = fslogic_event:emit_file_attr_update({uuid, FileUUID}, []),
    ok = file_consistency:add_components_and_notify(FileUUID, [file_meta]),
    ok = file_consistency:check_and_add_components(FileUUID, SpaceId, [parent_links]);
change_replicated_internal(SpaceId, Change = #change{model = file_location, doc = Doc = #document{value = #file_location{uuid = FileUUID}}}) ->
    ?info("change_replicated_internal: changed file_location ~p", [FileUUID]),
    ok = file_consistency:wait(FileUUID, SpaceId, [file_meta, local_file_location], [SpaceId, Change]),
    ok = replica_dbsync_hook:on_file_location_change(SpaceId, Doc);
change_replicated_internal(SpaceId, #change{model = file_meta, doc = #document{value = #links{model = file_meta, doc_key = FileUUID}}}) ->
    ?info("change_replicated_internal: changed links ~p", [FileUUID]),
    ok = file_consistency:check_and_add_components(FileUUID, SpaceId, [link_to_parent, parent_links]);
change_replicated_internal(_SpaceId, _Change) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Hook that runs just after link change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec links_changed(Origin :: links_utils:scope(), ModelName :: model_behaviour:model_type(),
    MainDocKey :: datastore:ext_key(), AddedMap :: #{}, DeletedMap :: #{}) ->
    ok.
links_changed(_Origin, ModelName = file_meta, MainDocKey, AddedMap, DeletedMap) ->
    #model_config{link_store_level = LinkStoreLevel} = ModelName:model_init(),
%%    critical_section:run([?MODULE, term_to_binary({links, MainDocKey})], fun() ->
            try
                MyProvID = oneprovider:get_provider_id(),
                erlang:put(mother_scope, MyProvID),
                erlang:put(other_scopes, []),

                ?info("YEY ~p", [{MainDocKey, AddedMap, DeletedMap}]),

                maps:fold(
                    fun(K, {Version, Targets} = V, AccIn) ->
                        ?info("Add forigin link ~p", [{MainDocKey, {K, V}}]),
                        NewTargets = lists:filter(
                            fun({_, _, Scope}) ->
                                case Scope of
                                    MyProvID ->
                                        false;
                                    _ ->
                                        true
                                end
                            end, Targets),
                        case NewTargets of
                            [] -> AccIn;
                            _ ->
                                ok = datastore:add_links(?DISK_ONLY_LEVEL, MainDocKey, ModelName, [{K, {Version, NewTargets}}])

                        end
                    end, [], AddedMap),

                maps:fold(
                    fun(K, V, _AccIn) ->
                        {_, DelTargets} = V,
                        ?info("Del forigin link ~p", [{MainDocKey, {K, V}}]),
                        lists:foreach(
                            fun({_, _, S}) ->
                                ok = datastore:delete_links(?DISK_ONLY_LEVEL, MainDocKey, ModelName, [links_utils:make_scoped_link_name(K, S, size(S))])
                            end, DelTargets)
                    end, [], DeletedMap),

                ok


            catch
                _:Reason ->
                    ?error_stacktrace("links_changed error ~p", [Reason])
            end,
%%        end),
    ok;
links_changed(_, _, _, _, _) ->
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