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
-include_lib("cluster_worker/include/modules/datastore/datastore_engine.hrl").
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
-spec change_replicated(SpaceId :: binary(), datastore:document()) ->
    any().
change_replicated(SpaceId, Change) ->
    true = is_supported(SpaceId),
    change_replicated_internal(SpaceId, Change).

%%--------------------------------------------------------------------
%% @doc
%% Hook that runs just after change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated_internal(od_space:id(), datastore:document()) ->
    any() | no_return().
change_replicated_internal(SpaceId, #document{
        key = FileUuid,
        value = #file_meta{type = ?REGULAR_FILE_TYPE, owner = UserId},
        deleted = true
    } = FileDoc) ->
    ?debug("change_replicated_internal: deleted file_meta ~p", [FileUuid]),
    case model:execute_with_default_context(
        file_meta, exists, [FileUuid], [{hooks_config, no_hooks}]
    ) of
        {ok, false} ->
            try
                FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
                % TODO - if links delete comes before, it fails!
                sfm_utils:delete_storage_file_without_location(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
                file_location:delete(file_location:local_id(FileUuid), UserId)
            catch
                _:{badmatch, {error, {not_found, file_meta}}} ->
                    % TODO - if links delete comes before, this function fails!
                    ok
            end;
        _ ->
            ok
    end;
change_replicated_internal(SpaceId, #document{
        key = FileUuid,
        value = #file_meta{type = ?REGULAR_FILE_TYPE}
    } = FileDoc) ->
    ?debug("change_replicated_internal: changed file_meta ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []);
change_replicated_internal(SpaceId, #document{
        key = FileUuid,
        % TODO - emit when file is deleted (for deleted files it fails)
        deleted = false,
        value = #file_meta{}
    } = FileDoc) ->
    ?debug("change_replicated_internal: changed file_meta ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []);
change_replicated_internal(SpaceId, #document{
        deleted = false,
        value = #file_location{uuid = FileUuid}
    } = Doc) ->
    ?debug("change_replicated_internal: changed file_location ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)),
    ok = replica_dbsync_hook:on_file_location_change(FileCtx, Doc);
change_replicated_internal(_SpaceId, #document{
        value = #links{
            model = file_meta,
            doc_key = FileUuid
        }
    }) ->
    ?debug("change_replicated_internal: changed links ~p", [FileUuid]);
change_replicated_internal(SpaceId, #document{
        key = FileUuid,
        value = #times{}
    }) ->
    ?debug("change_replicated_internal: changed times ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(FileUuid, SpaceId)),
    (catch fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx));
change_replicated_internal(_SpaceId, #document{
        key = FileUuid,
        value = #custom_metadata{}
    }) ->
    ?debug("change_replicated_internal: changed custom_metadata ~p", [FileUuid]);
change_replicated_internal(_SpaceId, Transfer = #document{value = #transfer{
    file_uuid = FileUuid
}}) ->
    ?debug("change_replicated_internal: changed transfer ~p", [FileUuid]),
    transfer_controller:on_transfer_doc_change(Transfer),
    invalidation_controller:on_transfer_doc_change(Transfer);
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
                            ok = model:execute_with_default_context(
                                MC, add_links,
                                [MainDocKey, [{K, {Version, NewTargetsAdd}}]],
                                [{hooks_config, no_hooks}, {link_replica_scope,
                                    ?DEFAULT_LINK_REPLICA_SCOPE}
                                ]
                            )
                    end,

                    %% Handle links marked as deleted
                    lists:foreach(
                        fun({Scope0, {deleted, VH0}, _, _}) ->
                            ok = model:execute_with_default_context(
                                MC, delete_links,
                                [MainDocKey, [links_utils:make_scoped_link_name(K,
                                    Scope0, VH0, size(Scope0))]],
                                [{hooks_config, no_hooks}]
                            )
                        end, NewTargetsDel)

            end
        end, #{}, AddedMap),

    maps:fold(
        fun(K, V, _AccIn) ->
            {_, DelTargets} = V,
            lists:foreach(
                fun({S, VH0, _, _}) ->
                    case VH0 of
                        {deleted, _VH1} ->
                            ok; %% Ignore deletion of deleted link
                        VH1 ->
                            ok = model:execute_with_default_context(
                                MC, delete_links,
                                [MainDocKey, [links_utils:make_scoped_link_name(K,
                                    S, VH1, size(S))]]
                            )
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
    lists:member(oneprovider:get_provider_id(),
        dbsync_utils:get_providers_for_space(SpaceId)).