%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing DBSync hooks for file meta.
%%% Uses file_meta_posthooks (see file_meta_posthooks.erl) to handle following situations:
%%%     *  hardlink file_meta document is synchronized before file_meta of file it references;
%%%     *  file_meta document is synchronized before its link from parent (fixes race with directory
%%%        listing and emitting events for new files).
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_file_meta_handler).
-author("Michal Stanisz").

-behaviour(file_meta_posthooks_behaviour).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    file_meta_change_replicated/2,
    hardlink_change_replicated/2
]).

%% file_meta_posthooks_behaviour
-export([
    encode_file_meta_posthook_args/2,
    decode_file_meta_posthook_args/2
]).

%% file_meta posthooks
-export([
    emit_file_changed_event/3
]).

%% for tests
-export([
    hardlink_replicated/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec file_meta_change_replicated(od_space:id(), file_meta:doc()) -> ok.
file_meta_change_replicated(SpaceId, #document{key = FileUuid, value = #file_meta{mode = CurrentMode}} = FileMetaDoc) ->
    FileCtx = file_ctx:new_by_doc(FileMetaDoc, SpaceId),
    {ok, FileCtx2} = sd_utils:chmod(FileCtx, CurrentMode),
    file_meta_change_replicated_internal(FileMetaDoc, FileCtx2),
    ok = file_meta_posthooks:execute_hooks(FileUuid, doc).


-spec hardlink_change_replicated(od_space:id(), file_meta:doc()) -> ok.
hardlink_change_replicated(SpaceId, #document{key = FileUuid} = LinkDoc) ->
    case file_meta:get_including_deleted(fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, ReferencedDoc} ->
            {ok, MergedDoc} = file_meta_hardlinks:merge_link_and_file_doc(LinkDoc, ReferencedDoc),
            FileCtx = file_ctx:new_by_doc(MergedDoc, SpaceId),
            % call using ?MODULE for mocking in tests
            ?MODULE:hardlink_replicated(LinkDoc, FileCtx);
        {error, not_found} ->
            add_hardlink_missing_base_doc_posthook(SpaceId, FileUuid);
        Error ->
            ?warning(?autoformat_with_msg("hardlink replicated ~tp - dbsync posthook failed", [FileUuid], Error))
    end.


%%%===================================================================
%%% file_meta_posthooks_behaviour
%%%===================================================================

-spec encode_file_meta_posthook_args(file_meta_posthooks:function_name(), [term()]) ->
    file_meta_posthooks:encoded_args().
encode_file_meta_posthook_args(_, Args) ->
    term_to_binary(Args).


-spec decode_file_meta_posthook_args(file_meta_posthooks:function_name(), file_meta_posthooks:encoded_args()) ->
    [term()].
decode_file_meta_posthook_args(hardlink_change_replicated, EncodedArgs) ->
    [SpaceId, HardlinkUuid] = binary_to_term(EncodedArgs),
    {ok, HardlinkDoc} = file_meta:get_including_deleted(HardlinkUuid),
    [SpaceId, HardlinkDoc];
decode_file_meta_posthook_args(emit_file_changed_event, EncodedArgs) ->
    [FileGuid, ParentUuid, Name] = binary_to_term(EncodedArgs),
    [file_ctx:new_by_guid(FileGuid), ParentUuid, Name].


%%%===================================================================
%%% file_meta posthooks
%%%===================================================================

-spec emit_file_changed_event(file_ctx:ctx(), file_meta:uuid(), file_meta:name()) -> ok.
emit_file_changed_event(FileCtx, ParentUuid, Name) ->
    case file_meta_forest:get(ParentUuid, all, Name) of
        {ok, _} ->
            ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []);
        {error, not_found} ->
            add_missing_parent_link_posthook(FileCtx, ParentUuid, Name)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec hardlink_replicated(file_meta:doc(), file_ctx:ctx()) -> ok | no_return().
hardlink_replicated(#document{
    value = #file_meta{deleted = Del1},
    deleted = Del2
}, FileCtx) when Del1 or Del2 ->
    fslogic_delete:handle_remotely_deleted_file(FileCtx);
hardlink_replicated(#document{
    key = FileUuid,
    value = #file_meta{name = Name, parent_uuid = ParentUuid}
}, FileCtx) ->
    % TODO VFS-7914 - Do not invalidate cache, when it is not needed
    ok = qos_logic:invalidate_cache_and_reconcile(FileCtx),
    ok = file_meta_posthooks:execute_hooks(FileUuid, doc),
    ok = emit_file_changed_event(FileCtx, ParentUuid, Name).


%% @private
-spec file_meta_change_replicated_internal(file_meta:doc(), file_ctx:ctx()) -> ok | no_return().
file_meta_change_replicated_internal(#document{
    value = #file_meta{deleted = Del1},
    deleted = Del2
}, FileCtx) when Del1 or Del2 ->
    fslogic_delete:handle_remotely_deleted_file(FileCtx);
file_meta_change_replicated_internal(#document{
    value = #file_meta{name = Name, parent_uuid = ParentUuid}
}, FileCtx) ->
    ok = emit_file_changed_event(FileCtx, ParentUuid, Name).


%% @private
-spec add_hardlink_missing_base_doc_posthook(od_space:id(), file_meta:uuid()) -> ok.
add_hardlink_missing_base_doc_posthook(SpaceId, HardlinkUuid) ->
    FileUuid = fslogic_file_id:ensure_referenced_uuid(HardlinkUuid),
    file_meta_posthooks:add_hook(
        ?MISSING_FILE_META(FileUuid),
        <<"hardlink missing base doc">>,
        SpaceId,
        ?MODULE,
        hardlink_change_replicated,
        [SpaceId, HardlinkUuid]
    ).


%% @private
-spec add_missing_parent_link_posthook(file_ctx:ctx(), file_meta:uuid(), file_meta:name()) -> ok.
add_missing_parent_link_posthook(FileCtx, ParentUuid, Name) ->
    file_meta_posthooks:add_hook(
        ?MISSING_FILE_LINK(ParentUuid, Name),
        <<"missing_parent_link_", Name/binary>>,
        file_ctx:get_space_id_const(FileCtx),
        ?MODULE,
        emit_file_changed_event,
        [file_ctx:get_logical_guid_const(FileCtx), ParentUuid, Name]
    ).
