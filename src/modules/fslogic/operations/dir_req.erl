%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon, Michał Stanisz
%%% @copyright (C) 2016-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on directories.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").
-author("Michal Stanisz").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").


%% API
-export([
    mkdir/4,
    create_dir_at_path/3,
    get_children_ctxs/3,
    get_children_attrs/4,
    list_recursively/4
]).

%% Deprecated API
-export([
    get_children/3
]).

-type recursive_listing_opts() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => recursive_listing:pagination_token(),
    start_after_path => recursive_file_listing_node:node_path(),
    prefix => recursive_listing:prefix(),
    limit => recursive_listing:limit(),
    include_directories => boolean()
}.

-type get_attr_fun() ::
    fun((user_ctx:ctx(), file_ctx:ctx(), file_attr:resolve_opts()) -> fslogic_worker:fuse_response()).
-type gather_attributes_mapper_fun(Entry, Attributes) ::
    fun((Entry, get_attr_fun(), file_attr:resolve_opts()) -> Attributes).

-export_type([recursive_listing_opts/0]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).

%%%===================================================================
%%% API
%%%===================================================================

-spec mkdir(
    user_ctx:ctx(),
    ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()
) ->
    fslogic_worker:fuse_response().
mkdir(UserCtx, ParentFileCtx0, Name, Mode) ->
    % TODO VFS-7064 this assert won't be needed after adding link from space to trash directory
    file_ctx:assert_not_trash_or_tmp_dir_const(ParentFileCtx0, Name),
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, ParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?add_subcontainer_mask)]
    ),
    mkdir_insecure(UserCtx, ParentFileCtx1, Name, Mode).


-spec create_dir_at_path(user_ctx:ctx(), file_ctx:ctx(), file_meta:path()) -> 
    fslogic_worker:fuse_response().
create_dir_at_path(UserCtx, RootFileCtx, Path) ->
    #fuse_response{fuse_response = #guid{guid = Guid}} =
        guid_req:ensure_dir(UserCtx, RootFileCtx, Path, ?DEFAULT_DIR_MODE),
    try attr_req:get_file_attr(UserCtx, file_ctx:new_by_guid(Guid), ?DEFAULT_ATTRS ++ [size]) of
        % if dir does not exist, it will be created during error handling
        #fuse_response{fuse_response = #file_attr{type = ?DIRECTORY_TYPE}} = Response ->
            Response;
        _ ->
            #fuse_response{status = #status{code = ?ENOTDIR}}
    catch Class:Reason ->
        case datastore_runner:normalize_error(Reason) of
            not_found -> create_dir_at_path(UserCtx, RootFileCtx, Path);
            ?ENOENT -> create_dir_at_path(UserCtx, RootFileCtx, Path);
            _ -> erlang:apply(erlang, Class, [Reason])
        end
    end.


%% @TODO VFS-11299 deprecated, left for compatibility with oneclient
-spec get_children(user_ctx:ctx(), file_ctx:ctx(), file_listing:options()) ->
    fslogic_worker:fuse_response().
get_children(UserCtx, FileCtx0, ListOpts) ->
    case get_children_attrs(UserCtx, FileCtx0, ListOpts, [guid, size]) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = FuseResponse} ->
            #file_children_attrs{child_attrs = ChildrenAttrs, pagination_token = PaginationToken} = FuseResponse,
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_children{
                    child_links = [#child_link{name = Name, guid = Guid} || #file_attr{name = Name, guid = Guid} <- ChildrenAttrs],
                    pagination_token = PaginationToken
                }
            };
        ErrorReponse ->
            ErrorReponse
    end.


-spec get_children_attrs(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(), [file_attr:attribute()]) ->
    fslogic_worker:fuse_response().
get_children_attrs(UserCtx, FileCtx0, ListOpts, Attributes) ->
    DirOperationsRequirements = case Attributes -- [guid, name] of
        [] -> ?OPERATIONS(?list_container_mask);
        _ -> ?OPERATIONS(?traverse_container_mask, ?list_container_mask)
    end,
    {Children, PaginationToken, FileCtx1} = get_children_ctxs(UserCtx, FileCtx0, ListOpts, DirOperationsRequirements),
    
    MapperFun = fun(ChildCtx, GetAttrFun, Opts) ->
        #fuse_response{status = #status{code = ?OK}, fuse_response = FileAttr} =
            GetAttrFun(UserCtx, ChildCtx, Opts),
        FileAttr
    end,

    ChildrenAttrs = gather_attributes(MapperFun, Children, #{attributes => Attributes}),
    
    fslogic_times:update_atime(FileCtx1),
    
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_attrs{
            child_attrs = ChildrenAttrs,
            pagination_token = PaginationToken
        }
    }.


-spec get_children_ctxs(user_ctx:ctx(), file_ctx:ctx(), file_listing:options()
) ->
    {
        ChildrenCtxs :: [file_ctx:ctx()],
        file_listing:pagination_token(),
        NewFileCtx :: file_ctx:ctx()
    }.
get_children_ctxs(UserCtx, FileCtx0, ListOpts) ->
    get_children_ctxs(
        UserCtx, FileCtx0, ListOpts, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)).


-spec list_recursively(user_ctx:ctx(), file_ctx:ctx(), recursive_listing_opts(), [file_attr:attribute()]) ->
    fslogic_worker:fuse_response().
list_recursively(UserCtx, FileCtx0, ListOpts, Attributes) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {_CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    list_recursively_insecure(UserCtx, FileCtx2, ListOpts, Attributes).
    

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir_insecure(
    user_ctx:ctx(),
    ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()
) ->
    fslogic_worker:fuse_response().
mkdir_insecure(UserCtx, ParentFileCtx, Name, Mode) ->
    ParentFileCtx2 = file_ctx:assert_not_readonly_storage(ParentFileCtx),
    ParentFileCtx3 = file_ctx:assert_is_dir(ParentFileCtx2),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx3),
    Owner = user_ctx:get_user_id(UserCtx),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentFileCtx3),
    {IsSyncEnabled, ParentFileCtx4} = file_ctx:is_synchronization_enabled(ParentFileCtx3),
    File = file_meta:new_doc(undefined, Name, ?DIRECTORY_TYPE, Mode, Owner, ParentUuid, SpaceId, not IsSyncEnabled),
    {ok, #document{key = DirUuid}} = file_meta:create({uuid, ParentUuid}, File),
    FileCtx = file_ctx:new_by_uuid(DirUuid, SpaceId),

    try
        {ok, Time} = times:save_with_current_times(DirUuid, SpaceId, not IsSyncEnabled),
        dir_update_time_stats:report_update_of_dir(file_ctx:get_logical_guid_const(FileCtx), Time),
        fslogic_times:update_mtime_ctime(ParentFileCtx4),

        #fuse_response{fuse_response = FileAttr} =
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, #{
                allow_deleted_files => false,
                name_conflicts_resolution_policy => allow_name_conflicts,
                attributes => ?DEFAULT_ATTRS
            }),
        FileAttr2 = FileAttr#file_attr{size = 0},
        ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, [user_ctx:get_session_id(UserCtx)]),
        dir_size_stats:report_file_created(?DIRECTORY_TYPE, file_ctx:get_logical_guid_const(ParentFileCtx)),
        #fuse_response{status = #status{code = ?OK},
            fuse_response = #dir{guid = file_id:pack_guid(DirUuid, SpaceId)}
        }
    catch
        Class:Reason ->
            FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
            file_meta:delete(FileUuid),
            times:delete(FileUuid),
            erlang:Class(Reason)
    end.


%% @private
-spec get_children_ctxs(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(), data_access_control:requirement()) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
get_children_ctxs(UserCtx, FileCtx0, ListOpts, DirOperationsRequirements) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    %% TODO VFS-7149 untangle permissions_check and fslogic_worker
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, DirOperationsRequirements];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    {ChildrenCtxs, PaginationToken, FileCtx3} = file_tree:list_children(
        FileCtx2, UserCtx, ListOpts, CanonicalChildrenWhiteList),
    FinalChildrenCtxs = ensure_extended_name_in_edge_files(UserCtx, ChildrenCtxs),
    {FinalChildrenCtxs, PaginationToken, FileCtx3}.


%% @private
-spec ensure_extended_name_in_edge_files(user_ctx:ctx(), [file_ctx:ctx()]) -> [file_ctx:ctx()].
ensure_extended_name_in_edge_files(UserCtx, FilesBatch) ->
    TotalNum = length(FilesBatch),
    lists:filtermap(fun({Num, FileCtx}) ->
        case (Num == 1 orelse Num == TotalNum) of
            true ->
                % safeguard, as file_meta doc can be not synchronized yet
                ?catch_not_found(begin
                    {_, FileCtx2} = file_attr:resolve(UserCtx, FileCtx, #{
                        attributes => [name],
                        name_conflicts_resolution_policy => resolve_name_conflicts
                    }),
                    {true, FileCtx2}
                end, false);
            false ->
                % Other files than first and last don't need to resolve name
                % conflicts (to check for collisions) as list_children
                % (file_meta:tag_children to be precise) already did it
                {true, FileCtx}
        end
    end, lists_utils:enumerate(FilesBatch)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists recursively files ordered by path lexicographically. 
%% By default only non-directory (i.e regular, symlinks and hardlinks) are listed.
%% When options `include_directories` is set to true directory entries will be included in result.
%% For more details consult `recursive_listing` and `recursive_file_listing_node` module doc.
%% @end
%%--------------------------------------------------------------------
-spec list_recursively_insecure(
    user_ctx:ctx(), file_ctx:ctx(), recursive_listing_opts(), [file_attr:attribute()]
) ->
    fslogic_worker:fuse_response().
list_recursively_insecure(UserCtx, FileCtx, ListOpts, Attributes) ->
    FinalListOpts = kv_utils:move_found(
        include_directories,
        include_branching_nodes,
        ListOpts
    ),
    #recursive_listing_result{
        entries = Entries
    } = Result = recursive_listing:list(recursive_file_listing_node, UserCtx, FileCtx, FinalListOpts),
    
    MapperFun = fun({Path, EntryFileCtx}, GetAttrFun, Opts) ->
        #fuse_response{status = #status{code = ?OK}, fuse_response = FileAttrs} =
            GetAttrFun(UserCtx, EntryFileCtx, Opts),
        {Path, FileAttrs}
    end,
    MappedEntries = gather_attributes(MapperFun, Entries, #{attributes => Attributes}),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = Result#recursive_listing_result{entries = MappedEntries}
    }.


%%--------------------------------------------------------------------
%% @doc
%% Calls GatherAttributesFun for every passed entry in parallel and
%% filters out entries for which it raised an error (potentially docs not
%% synchronized between providers or deleted files).
%% @end
%%--------------------------------------------------------------------
-spec gather_attributes(
    gather_attributes_mapper_fun(Entry, Attributes),
    [Entry],
    file_attr:resolve_opts()
) ->
    [Attributes].
gather_attributes(MapperFun, Entries, Opts) ->
    GetAttrFun = case file_attr:should_fetch_xattrs(Opts) of
        % fetching xattrs require more privileges than file listing, so insecure version cannot be called
        {true, _} -> fun attr_req:get_file_attr/3;
        false -> fun attr_req:get_file_attr_insecure/3
    end,
    FilterMapFun = fun(Entry) ->
        ?catch_not_found(begin
            {true, MapperFun(Entry, GetAttrFun, Opts)}
        end, false)
    end,
    lists_utils:pfiltermap(FilterMapFun, Entries, ?MAX_MAP_CHILDREN_PROCESSES).
