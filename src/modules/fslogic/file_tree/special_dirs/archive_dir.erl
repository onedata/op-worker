%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Root directory of an archive - for more details @see archivisation_tree.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_dir).
-author("Michal Stanisz").

-behaviour(special_dir_behaviour).

-dialyzer({nowarn_function, allowed_operations/0}).

-include("middleware/middleware.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

% API
-export([uuid/1, guid/2, ensure_exists/4]).
% special_dir_behaviour
-export([
    is_special/2,
    allowed_operations/0,
    is_filesystem_root_dir/0,
    can_be_shared/0,
    is_affected_by_protection_flags/0,
    is_included_in_harvesting/0,
    is_included_in_dir_stats/0,
    is_included_in_events/0,
    is_logically_detached/0,
    exists/1
]).


-define(ALLOWED_OPERATIONS, [
    #resolve_guid{},
    #get_file_path{},
    #resolve_guid_by_relative_path{},

    #get_file_attr{},
    #get_file_children{},
    #get_child_attr{},
    #get_file_children_attrs{},
    #get_recursive_file_list{},

    #historical_dir_size_stats_get_request{},

    % Operations needed for a provider to create an archive. After it is created, archive content is protected with
    % protection flags and its modification is no longer possible.
    #create_dir{},
    #create_file{},
    #make_file{},
    #make_link{},
    #make_symlink{},

    #custom_metadata_get_request{},

    % Needed for a protection flags check.
    #file_eff_dataset_summary_get_request{}
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec uuid(archive:id()) -> file_meta:uuid().
uuid(ArchiveId) -> ?ARCHIVE_DIR_UUID(ArchiveId).


-spec guid(od_space:id(), archive:id()) -> file_meta:uuid().
guid(SpaceId, ArchiveId) -> file_id:pack_guid(uuid(ArchiveId), SpaceId).


-spec ensure_exists(archive:id(), dataset:id(), od_space:id(), od_user:id()) -> ok.
ensure_exists(ArchiveId, DatasetId, SpaceId, ArchiveCreatorId) ->
    ParentUuid = dataset_archives_dir:uuid(DatasetId),
    FMDoc = file_meta:new_dir_doc(uuid(ArchiveId), ?ARCHIVE_DIR_NAME(ArchiveId), ?DEFAULT_DIR_PERMS,
        ArchiveCreatorId, ParentUuid, SpaceId
    ),
    dataset_archives_dir:ensure_exists(DatasetId, SpaceId),
    special_dir_docs:create(SpaceId, FMDoc, add_link),
    ok.


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, ?ARCHIVE_DIR_UUID(_ArchiveId)) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec allowed_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
allowed_operations() -> ?ALLOWED_OPERATIONS.


-spec is_filesystem_root_dir() -> boolean().
is_filesystem_root_dir() -> false.


-spec can_be_shared() -> boolean().
can_be_shared() -> false.


-spec is_affected_by_protection_flags() -> boolean().
is_affected_by_protection_flags() ->
    % Needed for protection flags to block any changes in archive after it is created.
    true.


-spec is_included_in_harvesting() -> boolean().
is_included_in_harvesting() -> false.


-spec is_included_in_dir_stats() -> boolean().
is_included_in_dir_stats() -> false.


-spec is_included_in_events() -> boolean().
is_included_in_events() -> true.


-spec is_logically_detached() -> boolean().
is_logically_detached() -> false.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).

