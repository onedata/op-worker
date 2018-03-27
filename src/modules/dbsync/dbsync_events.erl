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
    true = dbsync_utils:is_supported(SpaceId, [oneprovider:get_id()]),
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
    value = #file_meta{
        type = ?REGULAR_FILE_TYPE,
        deleted = Del1},
    deleted = Del2
} = FileDoc) when Del1 or Del2 ->
    ?debug("change_replicated_internal: deleted file_meta ~p", [FileUuid]),
    Ctx = datastore_model_default:get_ctx(file_meta),

    Proceed = case datastore_model:get(Ctx, FileUuid) of
        {error, not_found} ->
            true;
        {ok, #document{value = #file_meta{deleted = Del}} = X} ->
            Del
    end,

    case Proceed of
        true ->
            FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId, undefined),
            fslogic_deletion_worker:request_remote_deletion(FileCtx),
            ok;
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
    transfer_changes:handle(Transfer);
change_replicated_internal(_SpaceId, _Change) ->
    ok.