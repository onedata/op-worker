%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for trash management.
%%% WRITEME napisac, ze kosz ma konkretną nazwę i uuid
%%% WRITEME napisac, ze kosz ma miec docelowo zduplikowane linki i ze beda filtrowane w momencie rozwiazywania konfliktow przy ls
%%% @end
%%%-------------------------------------------------------------------
-module(trash).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([create/1, move_to_trash/1, delete_from_trash/4]).

% todo dodac test do usuwania w trakcie importu

%% TODO TESTY !!!!!!!!
% TODO pamietac o branchu w CW !!!!!
% TODO co z harvestowaniem kosza????
% TODO co z ustawianiem metadanych pod CMDI? (cdmi_metadata_req) Czy blokujemy?
% todo Czy pozwalamy zrobić share'a z kosza?
% todo Czy zakazujemy ustawiać QoS na koszu?

-define(NAME_UUID_SEPARATOR, "@@").
-define(NAME_IN_TRASH(FileName, FileUuid), <<FileName/binary, ?NAME_UUID_SEPARATOR, FileUuid/binary>>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(od_space:id()) -> ok.
create(SpaceId) ->
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    TrashDoc = file_meta:new_doc(fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
        ?TRASH_DIR_NAME, ?DIRECTORY_TYPE, ?DEFAULT_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId),
        SpaceUuid, SpaceId
    ),
    % TODO VFS-7064 use file_meta:create so that link to the trash directory will be added
    % TODO * remember to filter trash from list result in storage_import_deletion or replica_controller, tree_traverse, etc
    % TODO * maybe there should be option passed to file_meta_links:list that would exclude trash from the result
    %% {ok, _} = file_meta:create({uuid, SpaceUuid}, TrashDoc),
    {ok, _} = file_meta:save(TrashDoc),
    ok.


-spec move_to_trash(file_ctx:ctx()) -> file_ctx:ctx().
move_to_trash(FileCtx) ->
    file_ctx:assert_not_protected_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    FileCtx3 = maybe_add_deletion_marker(ParentUuid, FileCtx2),
    {Name, FileCtx4} = file_ctx:get_aliased_name(FileCtx3, UserCtx),
    {FileDoc, FileCtx5} = file_ctx:get_file_doc(FileCtx4),
    % files moved to trash are children of trash directory
    % they names are suffixed with Uuid to avoid conflicts
    TrashUuid = fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
    % TODO VFS-7133 save original parent after extending file_meta
    file_qos:clean_up(FileCtx),
    file_meta:rename(FileDoc, ParentUuid, TrashUuid, ?NAME_IN_TRASH(Name, Uuid)),
    FileCtx5.


-spec delete_from_trash(file_ctx:ctx(), user_ctx:ctx(), boolean(), file_meta:uuid()) ->
    {ok, tree_deletion_traverse:id()}.
delete_from_trash(FileCtx, UserCtx, Silent, RootOriginalParentUuid) ->
    file_ctx:assert_not_protected_const(FileCtx),
    % TODO VFS-7101 use offline access token in tree_traverse
    tree_deletion_traverse:start(FileCtx, UserCtx, Silent, RootOriginalParentUuid).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_add_deletion_marker(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
maybe_add_deletion_marker(ParentUuid, FileCtx) ->
    case file_ctx:is_imported_storage(FileCtx) of
        {true, FileCtx2} -> deletion_marker:add(ParentUuid, FileCtx2);
        {false, FileCtx2} -> FileCtx2
    end.