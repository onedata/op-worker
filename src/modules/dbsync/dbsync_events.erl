%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc DBSync hooks.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_events).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([change_replicated/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for change_replicated_internal, ignoring unsupported spaces.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated(SpaceId :: binary(), datastore:doc()) ->
    any().
change_replicated(SpaceId, Change) ->
    true = dbsync_utils:is_supported(SpaceId, [oneprovider:get_id(fail_with_throw)]),
    change_replicated_internal(SpaceId, Change).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Hook that runs just after change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated_internal(od_space:id(), datastore:doc()) ->
    any() | no_return().
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{type = ?REGULAR_FILE_TYPE, owner = UserId},
    deleted = true
} = FileDoc) ->
    ?debug("change_replicated_internal: deleted file_meta ~p", [FileUuid]),
    Ctx = datastore_model_default:get_ctx(file_meta),
    case datastore_model:exists(Ctx, FileUuid) of
        {ok, false} ->
            try
                FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
                fslogic_event_emitter:emit_file_removed(FileCtx, []),
                % TODO - if links delete comes before, it fails!
                sfm_utils:delete_storage_file_without_location(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
                file_location:delete(file_location:local_id(FileUuid), UserId)
            catch
                _:{badmatch, {error, not_found}} ->
                    % TODO - if links delete comes before, this function fails!
                    ok
            end;
        _ ->
            ok
    end;
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{},
    deleted = true
} = FileDoc) ->
    ?debug("change_replicated_internal: deleted file_meta (directory) ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
    fslogic_event_emitter:emit_file_removed(FileCtx, []);
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{deleted = true}
} = FileDoc) ->
    ?debug("change_replicated_internal: deleted file_meta (internal delete field is true) ~p",
        [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
    fslogic_event_emitter:emit_file_removed(FileCtx, []);
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{type = ?REGULAR_FILE_TYPE}
} = FileDoc) ->
    ?debug("change_replicated_internal: changed file_meta ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []);
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
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