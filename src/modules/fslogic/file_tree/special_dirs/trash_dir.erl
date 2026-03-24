%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for trash management.
%%% Trash is a special directory where files are moved as a result
%%% of fslogic #move_to_trash operation.
%%% Trash directory is created for each space, it has a predefined uuid and a predefined name.
%%% Trash directory is child of a space directory.
%%%
%%% TODO VFS-7064 below paragraph will be true after adding link from space directory to trash
%%% Each provider adds link from space to trash to its own file_meta_forest
%%% tree. The links are not listed as conflicting because file_meta_forest
%%% module detects that they point to the same uuid.
%%%
%%% Currently, deletion using trash is performed on directories via GUI
%%% and REST/CDMI interfaces.
%%%
%%% Directories moved to trash become direct children of the trash directory.
%%% Therefore, their names are suffixed with their uuid to avoid conflicts.
%%% @end
%%%-------------------------------------------------------------------
-module(trash_dir).
-author("Jakub Kudzia").

-behaviour(special_dir_behaviour).

% ignore this function as it uses record definitions without setting fields values
-dialyzer({nowarn_function, supported_operations/0}).

-include("middleware/middleware.hrl").
-include("proto/oneclient/fuse_messages.hrl").


%% API
-export([uuid/1, guid/1, ensure_exists/1]).
-export([move_to_trash/2, schedule_deletion_from_trash/5]).
-export([is_name_allowed/2]).

% special_dir_behaviour
-export([
    is_special/2,
    supported_operations/0,
    is_filesystem_root_dir/0,
    can_be_shared/0,
    is_affected_by_protection_flags/0,
    is_included_in_harvesting/0,
    is_included_in_dir_stats/0,
    is_included_in_events/0,
    is_logically_detached/0,
    exists/1
]).


%% Debug functions
-export([
    list/1, list/2,
    clear_all/1, clear_all/2, clear_all/3
]).


-define(NAME_UUID_SEPARATOR, "@@").
-define(NAME_IN_TRASH(FileName, FileUuid), <<FileName/binary, ?NAME_UUID_SEPARATOR, FileUuid/binary>>).


-define(SUPPORTED_OPERATIONS, [
    #resolve_guid{},

    #get_file_attr{},
    #get_file_children{},
    #get_child_attr{},
    #get_file_children_attrs{},
    #get_recursive_file_list{},

    #historical_dir_size_stats_get_request{}
]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec uuid(od_space:id()) -> file_meta:uuid().
uuid(SpaceId) ->
    ?TRASH_DIR_UUID(SpaceId).


-spec guid(od_space:id()) -> file_id:file_guid().
guid(SpaceId) ->
    file_id:pack_guid(uuid(SpaceId), SpaceId).


-spec ensure_exists(od_space:id()) -> ok.
ensure_exists(SpaceId) ->
    % TODO VFS-7064 use file_meta:create so that link to the trash directory will be added
    %  * remember to filter trash from list result in storage_import_deletion or replica_controller, tree_traverse, etc
    %  * maybe there should be option passed to file_meta_forest:list that would exclude trash from the result
    case special_dir_docs:create(SpaceId, prepare_doc(SpaceId), no_link) of
        created -> ?info("Created trash directory for space '~ts'", [SpaceId]);
        exists -> ok
    end.


-spec move_to_trash(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
move_to_trash(FileCtx, UserCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_tree:get_parent_guid_if_not_logically_detached(FileCtx, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    FileCtx3 = add_deletion_marker_if_applicable(ParentUuid, FileCtx2),
    {Name, FileCtx4} = file_ctx:get_aliased_name(FileCtx3, UserCtx),
    {FileDoc, FileCtx5} = file_ctx:get_file_doc(FileCtx4),
    % files moved to trash are direct children of trash directory
    % their names are suffixed with Uuid to avoid conflicts
    TrashUuid = uuid(SpaceId),
    % TODO VFS-7133 save original parent after extending file_meta !!!
    file_qos:cleanup_reference_related_documents(FileCtx5),
    ok = qos_eff_cache:invalidate_on_all_nodes(SpaceId),
    NameInTrash = ?NAME_IN_TRASH(Name, Uuid),
    file_meta:rename(FileDoc, ParentUuid, TrashUuid, NameInTrash),
    ok = dataset_eff_cache:invalidate_on_all_nodes(SpaceId),
    ok = paths_cache:invalidate_on_all_nodes(SpaceId),
    TrashGuid = file_id:pack_guid(TrashUuid, SpaceId),
    permissions_cache:invalidate(),
    fslogic_event_emitter:emit_file_renamed_no_exclude(
        FileCtx5, ParentGuid, TrashGuid, NameInTrash, Name),
    file_ctx:reset(FileCtx5). % file_meta document and parent cached in file_ctx are invalid


%%--------------------------------------------------------------------
%% @doc
%% This function starts a tree_deletion_traverse on given subtree to
%% asynchronously remove the subtree from the trash.
%% @end
%%--------------------------------------------------------------------
-spec schedule_deletion_from_trash(file_ctx:ctx(), user_ctx:ctx(), boolean(), file_meta:uuid(), file_meta:name()) ->
    {ok, tree_deletion_traverse:id()} | {error, term()}.
schedule_deletion_from_trash(FileCtx, _UserCtx, EmitEvents, RootOriginalParentUuid, RootFileName) ->
    % TODO VFS-7348 schedule deletion as user not by root
    case tree_deletion_traverse:start(FileCtx, user_ctx:new(?ROOT_USER_ID), EmitEvents, RootOriginalParentUuid, RootFileName) of
        {ok, TaskId} ->
            {ok, TaskId};
        {error, _} = Error ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ?error("Unable to start deletion of ~ts from trash in space ~ts due to ~tp.", [Guid, SpaceId, Error]),
            Error
    end.


%% @TODO VFS-7064 no longer needed after link between space and trash dir is created
-spec is_name_allowed(file_meta:name(), file_meta:uuid()) -> boolean().
is_name_allowed(?TRASH_DIR_NAME, ParentUuid) ->
    case space_dir:is_special(uuid, ParentUuid) of
        true -> false;
        false -> true
    end;
is_name_allowed(_, _ParentUuid) ->
    true.


%%%===================================================================
%%% special_dir_behaviour callbacks
%%%===================================================================

-spec is_special(uuid | guid, file_meta:uuid()) -> boolean().
is_special(uuid, <<?TRASH_DIR_UUID_PREFIX, _SpaceId/binary>>) -> true;
is_special(guid, Guid) -> is_special(uuid, file_id:guid_to_uuid(Guid));
is_special(_, _) -> false.


-spec supported_operations() -> [middleware_worker:operation() | fslogic_worker:operation()].
supported_operations() -> ?SUPPORTED_OPERATIONS.


-spec is_filesystem_root_dir() -> boolean().
is_filesystem_root_dir() -> false.


-spec can_be_shared() -> boolean().
can_be_shared() -> false.


-spec is_affected_by_protection_flags() -> boolean().
is_affected_by_protection_flags() -> false.


-spec is_included_in_harvesting() -> boolean().
is_included_in_harvesting() -> false.


-spec is_included_in_dir_stats() -> boolean().
is_included_in_dir_stats() -> false.


-spec is_included_in_events() -> boolean().
is_included_in_events() -> false.


-spec is_logically_detached() -> boolean().
is_logically_detached() -> true.


-spec exists(file_meta:uuid()) -> boolean().
exists(Uuid) ->
    file_meta:exists(Uuid).


%%%===================================================================
%%% Debug helpers - functions to be used in debug, should not be used in production code
%%%===================================================================

-spec list(od_space:id()) -> {[file_ctx:ctx()], file_listing:pagination_token()}.
list(SpaceId) ->
    list(SpaceId, file_listing:starting_opts_with_tune_for_cont_listing(false)).

-spec list(od_space:id(), file_listing:options()) -> {[file_ctx:ctx()], file_listing:pagination_token()}.
list(SpaceId, ListOpts) when is_map(ListOpts) ->
    {Children, NextPaginationToken, _} = dir_req:list_children_ctxs(user_ctx:new(?ROOT_SESS_ID),
        file_ctx:new_by_guid(trash_dir:guid(SpaceId)), ListOpts),
    {Children, NextPaginationToken};
list(SpaceId, PaginationToken) ->
    ListOpts = #{pagination_token => PaginationToken},
    list(SpaceId, ListOpts).


% NOTE: this is best effort and is not guaranteed to work properly (mainly due to not having original parent uuid)
-spec clear_all(od_space:id()) -> ok.
clear_all(SpaceId) ->
    clear_all(SpaceId, emit_events).

-spec clear_all(od_space:id(), emit_events | no_events) -> ok.
clear_all(SpaceId, EventsMode) ->
    clear_all(SpaceId, EventsMode, undefined).

-spec clear_all(od_space:id(), emit_events | no_events, file_listing:pagination_token() | undefined) -> ok.
clear_all(SpaceId, EventsMode, Token) ->
    EmitEventsFlag = EventsMode == emit_events,
    {List, NextToken} = case Token of
        undefined -> list(SpaceId);
        _ -> list(SpaceId, Token)
    end,
    lists:foreach(fun(FileCtx) ->
        % Cache deleted file meta in file_ctx so traverse can start on deleted file.
        {_, FileCtx1} = file_ctx:get_and_cache_file_doc_including_deleted(FileCtx),
        schedule_deletion_from_trash(FileCtx1, user_ctx:new(?ROOT_SESS_ID), EmitEventsFlag,
            space_dir:uuid(SpaceId), extract_name(FileCtx1))
    end, List),
    case file_listing:is_finished(NextToken) of
        true -> ok;
        false -> clear_all(SpaceId, EventsMode, NextToken)
    end.


-spec extract_name(file_ctx:ctx()) -> binary(). 
extract_name(FileCtx) ->
    {ExtendedName, _} = file_ctx:get_aliased_name(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
    str_utils:join_binary(
        lists:droplast(binary:split(ExtendedName, <<?NAME_UUID_SEPARATOR>>, [global])), <<?NAME_UUID_SEPARATOR>>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec prepare_doc(od_space:id()) -> file_meta:doc().
prepare_doc(SpaceId) ->
    file_meta:new_dir_doc(uuid(SpaceId),
        ?TRASH_DIR_NAME, ?DEFAULT_DIR_MODE, ?SPACE_OWNER_ID(SpaceId), space_dir:uuid(SpaceId), SpaceId
    ).


%% @private
-spec add_deletion_marker_if_applicable(file_meta:uuid(), file_ctx:ctx()) -> file_ctx:ctx().
add_deletion_marker_if_applicable(ParentUuid, FileCtx) ->
    case file_ctx:is_imported_storage(FileCtx) of
        {true, FileCtx2} ->
            case file_ctx:is_storage_file_created(FileCtx2) of
                {true, FileCtx3} -> deletion_marker:add(ParentUuid, FileCtx3);
                {false, FileCtx3} -> FileCtx3
            end;
        {false, FileCtx2} -> FileCtx2
    end.