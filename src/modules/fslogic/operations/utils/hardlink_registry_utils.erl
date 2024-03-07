%%%--------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This is utility module for hardlinks registration and deregistration.
%%% It is created because both link registration/deregistration and stats
%%% update have to be handled inside replica_synchronizer when stats
%%% are active. Thus, this module hides dependency between these concepts
%%% and can be deleted after replica_synchronizer refactoring (VFS-11341).
%%%
%%% This module handles also creation/deletion of hardlinks to opened deleted
%%% files. They are created in OPENED_DELETED_FILES_DIR to ensure that
%%% such files can be found in system e.g. to count its statistics.
%%% @end
%%%--------------------------------------------------------------------
-module(hardlink_registry_utils).
-author("Michal Wrzeszcz").


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([register/3, deregister/1,
    create_hidden_hardlink_for_opened_deleted_file/1, delete_hidden_hardlink_for_opened_deleted_file/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec register(file_ctx:ctx(), file_id:file_guid(), file_meta:uuid()) -> ok.
register(TargetFileCtx, TargetParentGuid, LinkUuid) ->
    FileUuid = file_ctx:get_logical_uuid_const(TargetFileCtx),
    apply_depending_on_stats_status(
        TargetFileCtx,
        fun() ->
            {ok, _} = file_meta_hardlinks:register(FileUuid, LinkUuid), % TODO VFS-7445 - revert after error
            dir_size_stats:on_link_register(TargetFileCtx, TargetParentGuid)
        end,
        fun() ->
            {ok, _} = file_meta_hardlinks:register(FileUuid, LinkUuid), % TODO VFS-7445 - revert after error
            ok
        end
    ).


-spec deregister(file_ctx:ctx()) -> file_meta_hardlinks:references_presence().
deregister(FileCtx) ->
    LinkUuid = file_ctx:get_logical_uuid_const(FileCtx),
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(ReferencedFileCtx),
    {ok, ReferencesPresence} = apply_depending_on_stats_status(
        ReferencedFileCtx,
        fun() ->
            dir_size_stats:on_link_deregister(FileCtx),
            file_meta_hardlinks:deregister(FileUuid, LinkUuid)
        end,
        fun() ->
            file_meta_hardlinks:deregister(FileUuid, LinkUuid)
        end
    ),
    ReferencesPresence.


-spec create_hidden_hardlink_for_opened_deleted_file(file_ctx:ctx()) -> ok.
create_hidden_hardlink_for_opened_deleted_file(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(ReferencedFileCtx),

    RegisterHidden = fun() ->
        ParentUuid = ?OPENED_DELETED_FILES_DIR_UUID(SpaceId),
        Doc = file_meta_hardlinks:new_doc(FileUuid, FileUuid, ParentUuid, SpaceId, true),
        LinkUuid = fslogic_file_id:gen_deleted_opened_file_link_uuid(FileUuid),
        file_meta:create({uuid, ParentUuid}, Doc#document{key = LinkUuid}),
        {ok, _} = file_meta_hardlinks:register(FileUuid, LinkUuid),
        LinkUuid
    end,

    apply_depending_on_stats_status(
        ReferencedFileCtx,
        fun() ->
            LinkUuid = RegisterHidden(),
            dir_size_stats:on_opened_file_delete(ReferencedFileCtx, LinkUuid)
        end,
        fun() ->
            RegisterHidden(),
            ok
        end
    ).


-spec delete_hidden_hardlink_for_opened_deleted_file(file_ctx:ctx()) -> ok.
delete_hidden_hardlink_for_opened_deleted_file(FileCtx) ->
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(ReferencedFileCtx),

    DeleteHidden = fun() ->
        case file_meta_hardlinks:list_references(FileUuid) of
            {ok, [LinkUuid | _]} ->
                file_meta_hardlinks:deregister(FileUuid, LinkUuid),
                file_meta:delete(LinkUuid),
                LinkUuid;
            {ok, []} ->
                undefined
        end
    end,

    apply_depending_on_stats_status(
        ReferencedFileCtx,
        fun() ->
            case DeleteHidden() of
                undefined -> ok;
                LinkUuid -> dir_size_stats:on_deleted_file_close(ReferencedFileCtx, LinkUuid)
            end
        end,
        fun() ->
            DeleteHidden(),
            ok
        end
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec apply_depending_on_stats_status(file_ctx:ctx(), fun(() -> Result), fun(() -> Result)) -> Result | no_return().
apply_depending_on_stats_status(FileCtx, ActiveStatsFun, DisabledStatsFun) ->
    case dir_stats_service_state:is_active(file_ctx:get_space_id_const(FileCtx)) of
        true ->
            ApplyAns = replica_synchronizer:apply(FileCtx, fun() ->
                try
                    ActiveStatsFun()
                catch
                    Class:Reason:Stacktrace -> {catched, ?examine_exception(Class, Reason, Stacktrace)}
                end
            end),

            case ApplyAns of
                {catched, Error} -> throw(Error);
                _ -> ApplyAns
            end;
        false ->
            DisabledStatsFun()
    end.