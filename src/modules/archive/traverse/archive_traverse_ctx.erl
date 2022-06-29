%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with helper structure used in archivisation traverse.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_traverse_ctx).
-author("Michal Stanisz").

-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").


%% API
-export([
    build/1, build/2,
    get_parent_file_ctx/1, get_archive_doc/1,
    set_target_parent/2, set_archive_doc/2,
    ensure_guid_in_ctx/1
]).

-record(archive_traverse_ctx, {
    current_archive_doc :: archive:doc() | undefined,
    target_parent :: file_id:file_guid() | file_ctx:ctx() | undefined
}).

-type ctx() :: #archive_traverse_ctx{}.

-export_type([ctx/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec build(archive:doc() | undefined) -> ctx().
build(undefined) ->
    #archive_traverse_ctx{};
build(ArchiveDoc) ->
    {ok, ArchiveDataDirGuid} = archive:get_data_dir_guid(ArchiveDoc),
    build(ArchiveDoc, ArchiveDataDirGuid).


-spec build(archive:doc() | undefined, file_ctx:ctx() | file_id:file_guid()) -> ctx().
build(ArchiveDoc, ArchiveTargetParent) ->
    #archive_traverse_ctx{
        current_archive_doc = ArchiveDoc,
        target_parent = ArchiveTargetParent
    }.


-spec get_parent_file_ctx(ctx()) -> file_ctx:ctx() | undefined.
get_parent_file_ctx(#archive_traverse_ctx{target_parent = undefined}) ->
    undefined;
get_parent_file_ctx(#archive_traverse_ctx{target_parent = Guid}) when is_binary(Guid) ->
    file_ctx:new_by_guid(Guid);
get_parent_file_ctx(#archive_traverse_ctx{target_parent = FileCtx})->
    FileCtx.


-spec get_archive_doc(ctx()) -> archive:doc() | undefined.
get_archive_doc(#archive_traverse_ctx{current_archive_doc = ArchiveDoc}) ->
    ArchiveDoc.


-spec set_archive_doc(ctx(), archive:doc() | undefined) -> ctx().
set_archive_doc(Ctx, NewDoc) ->
    Ctx#archive_traverse_ctx{current_archive_doc = NewDoc}.


-spec set_target_parent(ctx(), file_ctx:ctx() | file_id:file_guid()) -> ctx().
set_target_parent(Ctx, NewParent) ->
    Ctx#archive_traverse_ctx{target_parent = NewParent}.


-spec ensure_guid_in_ctx(ctx()) -> ctx().
ensure_guid_in_ctx(#archive_traverse_ctx{target_parent = undefined} = ArchiveCtx) ->
    ArchiveCtx;
ensure_guid_in_ctx(#archive_traverse_ctx{target_parent = Guid} = ArchiveCtx) when is_binary(Guid) ->
    ArchiveCtx;
ensure_guid_in_ctx(#archive_traverse_ctx{target_parent = FileCtx} = ArchiveCtx) ->
    ArchiveCtx#archive_traverse_ctx{target_parent = file_ctx:get_logical_guid_const(FileCtx)}.

