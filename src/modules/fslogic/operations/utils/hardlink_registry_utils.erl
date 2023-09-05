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
%%% and can be deleted after replica_synchronizer refactoring.
%%% @end
%%%--------------------------------------------------------------------
-module(hardlink_registry_utils).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/logging.hrl").


%% API
-export([register/3, deregister/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec register(file_ctx:ctx(), file_id:file_guid(), file_meta:uuid()) -> ok.
register(TargetFileCtx, TargetParentGuid, LinkUuid) ->
    FileUuid = file_ctx:get_logical_uuid_const(TargetFileCtx),
    SpaceId = file_id:guid_to_space_id(TargetParentGuid),
    case dir_stats_service_state:is_active(SpaceId) of
        true ->
            ok = replica_synchronizer:apply(TargetFileCtx, fun() ->
                try
                    {ok, _} = file_meta_hardlinks:register(FileUuid, LinkUuid), % TODO VFS-7445 - revert after error
                    dir_size_stats:on_link_register(TargetFileCtx, TargetParentGuid)
                catch
                    Class:Reason:Stacktrace -> ?examine_exception(Class, Reason, Stacktrace)
                end
            end);
        false ->
            {ok, _} = file_meta_hardlinks:register(FileUuid, LinkUuid), % TODO VFS-7445 - revert after error
            ok
    end.


-spec deregister(file_ctx:ctx()) -> file_meta_hardlinks:references_presence().
deregister(FileCtx) ->
    LinkUuid = file_ctx:get_logical_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    FileUuid = file_ctx:get_logical_uuid_const(ReferencedFileCtx),

    {ok, ReferencesPresence} = case dir_stats_service_state:is_active(SpaceId) of
        true ->
            replica_synchronizer:apply(ReferencedFileCtx, fun() ->
                dir_size_stats:on_link_deregister(FileCtx),
                file_meta_hardlinks:deregister(FileUuid, LinkUuid)
            end);
        false ->
            file_meta_hardlinks:deregister(FileUuid, LinkUuid)
    end,
    ReferencesPresence.