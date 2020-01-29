%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides functions operating on links
%%% (especially deletion links).
%%% @end
%%%--------------------------------------------------------------------
-module(link_utils).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_sufix.hrl").

%% API
-export([try_to_resolve_child_link/2, try_to_resolve_child_deletion_link/2,
    add_deletion_link/2, remove_deletion_link/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function tries to resolve child with name FileName of
%% directory associated with ParentCtx.
%% @end
%%-------------------------------------------------------------------
-spec try_to_resolve_child_link(file_meta:name(), file_ctx:ctx()) ->
    {ok, file_meta:uuid()} | {error, term()}.
try_to_resolve_child_link(FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    fslogic_path:to_uuid(ParentUuid, FileName).

%%-------------------------------------------------------------------
%% @doc
%% This function tries to resolve child's deletion_link
%% @end
%%-------------------------------------------------------------------
-spec try_to_resolve_child_deletion_link(file_meta:name(), file_ctx:ctx()) ->
    {ok, file_meta:uuid()} | {error, term()}.
try_to_resolve_child_deletion_link(FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    fslogic_path:to_uuid(ParentUuid, ?FILE_DELETION_LINK_NAME(FileName)).

%%-------------------------------------------------------------------
%% @doc
%% This function adds a deletion_link for the file.
%% @end
%%-------------------------------------------------------------------
-spec add_deletion_link(file_ctx:ctx(), file_meta:uuid()) -> file_ctx:ctx().
add_deletion_link(FileCtx, ParentUuid) ->
    {DeletionLinkName, FileCtx2} = file_deletion_link_name(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx2),
    Scope = file_ctx:get_space_id_const(FileCtx2),
    case file_meta:add_child_link(ParentUuid, Scope, DeletionLinkName, FileUuid) of
        ok -> ok;
        {error, already_exists} -> ok
    end,
    FileCtx2.

%%-------------------------------------------------------------------
%% @doc
%% This function remove a deletion_link for the file.
%% @end
%%-------------------------------------------------------------------
-spec remove_deletion_link(file_ctx:ctx(), file_meta:uuid()) -> file_ctx:ctx().
remove_deletion_link(FileCtx, ParentUuid) ->
    {DeletionLinkName, FileCtx2} = file_deletion_link_name(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx2),
    ok = file_meta:delete_deletion_link(ParentUuid, Scope, DeletionLinkName),
    FileCtx2.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Utility function that returns deletion_link name for given file.
%% @end
%%-------------------------------------------------------------------
-spec file_deletion_link_name(file_ctx:ctx()) -> {file_meta:name(), file_ctx:ctx()}.
file_deletion_link_name(FileCtx) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    BaseName = filename:basename(StorageFileId),
    {?FILE_DELETION_LINK_NAME(BaseName), FileCtx2}.