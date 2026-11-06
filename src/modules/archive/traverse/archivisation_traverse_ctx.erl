%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with helper structure used in archivisation traverse. 
%%% It consists of an archive document that is currently being processed 
%%% and a guid of a directory where the archived files are to be copied.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_traverse_ctx).
-author("Michal Stanisz").

-include("tree_traverse.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").


%% API
-export([
    init_for_new_traverse/1, init_for_nested_archive/2,
    get_target_parent/1, get_archive_doc/1,
    set_target_parent/2, set_archive_doc/2
]).

-record(archivisation_traverse_ctx, {
    current_archive_doc :: archive:doc() | undefined,
    target_parent :: file_id:file_guid() | undefined
}).

-type ctx() :: #archivisation_traverse_ctx{}.

-export_type([ctx/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_for_new_traverse(archive:doc() | undefined) -> ctx().
init_for_new_traverse(undefined) ->
    #archivisation_traverse_ctx{};
init_for_new_traverse(ArchiveDoc) ->
    {ok, ArchiveDataDirGuid} = archive:get_data_dir_guid(ArchiveDoc),
    init_for_nested_archive(ArchiveDoc, ArchiveDataDirGuid).


-spec init_for_nested_archive(archive:doc() | undefined, file_id:file_guid()) -> ctx().
init_for_nested_archive(ArchiveDoc, ArchiveTargetParent) ->
    #archivisation_traverse_ctx{
        current_archive_doc = ArchiveDoc,
        target_parent = ArchiveTargetParent
    }.


-spec get_target_parent(ctx()) -> file_id:file_guid() | undefined.
get_target_parent(#archivisation_traverse_ctx{target_parent = Guid}) ->
    Guid.


-spec get_archive_doc(ctx()) -> archive:doc() | undefined.
get_archive_doc(#archivisation_traverse_ctx{current_archive_doc = ArchiveDoc}) ->
    ArchiveDoc.


-spec set_archive_doc(ctx(), archive:doc() | undefined) -> ctx().
set_archive_doc(Ctx, NewDoc) ->
    Ctx#archivisation_traverse_ctx{current_archive_doc = NewDoc}.


-spec set_target_parent(ctx(), file_id:file_guid() | undefined) -> ctx().
set_target_parent(Ctx, NewParent) ->
    Ctx#archivisation_traverse_ctx{target_parent = NewParent}.
