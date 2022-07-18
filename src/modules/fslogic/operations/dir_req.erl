%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
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

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").


%% API
-export([
    mkdir/4,
    get_children_ctxs/3,
    get_children/3,
    get_children_attrs/5,
    get_children_details/3,
    list_recursively/3
]).

-type map_child_fun() :: fun((user_ctx:ctx(), file_ctx:ctx(), attr_req:compute_file_attr_opts()) ->
    fslogic_worker:fuse_response()) | no_return().

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
    file_ctx:assert_not_trash_dir_const(ParentFileCtx0, Name),
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, ParentFileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?add_subcontainer_mask)]
    ),
    mkdir_insecure(UserCtx, ParentFileCtx1, Name, Mode).


-spec get_children(user_ctx:ctx(), file_ctx:ctx(), file_listing:options()) ->
    fslogic_worker:fuse_response().
get_children(UserCtx, FileCtx0, ListOpts) ->
    ParentGuid = file_ctx:get_logical_guid_const(FileCtx0),
    {ChildrenCtxs, ListingToken, FileCtx1} = get_children_ctxs(
        UserCtx, FileCtx0, ListOpts, ?OPERATIONS(?list_container_mask)),
    ChildrenNum = length(ChildrenCtxs),

    ChildrenLinks = lists:filtermap(fun({Num, ChildCtx}) ->
        ChildGuid = file_ctx:get_logical_guid_const(ChildCtx),
        {ChildName, ChildCtx2} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
        case Num == 1 orelse Num == ChildrenNum of
            true ->
                try
                    {FileDoc, _ChildCtx3} = file_ctx:get_file_doc(ChildCtx2),
                    ProviderId = file_meta:get_provider_id(FileDoc),
                    {ok, FileUuid} = file_meta:get_uuid(FileDoc),
                    case file_meta:check_name_and_get_conflicting_files(
                        file_id:guid_to_uuid(ParentGuid), ChildName, FileUuid, ProviderId) 
                    of
                        {conflicting, ExtendedName, _ConflictingFiles} ->
                            {true, #child_link{name = ExtendedName, guid = ChildGuid}};
                        _ ->
                            {true, #child_link{name = ChildName, guid = ChildGuid}}
                    end
                catch _:_ ->
                    % Documents for file have not yet synchronized
                    false
                end;
            false ->
                % Other files than first and last don't need to resolve name
                % conflicts (to check for collisions) as list_children
                % (file_meta:tag_children to be precise) already did it
                {true, #child_link{name = ChildName, guid = ChildGuid}}
        end
    end, lists_utils:enumerate(ChildrenCtxs)),

    fslogic_times:update_atime(FileCtx1),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children{
            child_links = ChildrenLinks,
            pagination_token = ListingToken
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


%%--------------------------------------------------------------------
%% @equiv get_children_attrs_insecure/7 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(), boolean(), boolean()) ->
    fslogic_worker:fuse_response().
get_children_attrs(UserCtx, FileCtx0, ListOpts, IncludeReplicationStatus, IncludeLinkCount) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    get_children_attrs_insecure(
        UserCtx, FileCtx2, ListOpts, IncludeReplicationStatus, IncludeLinkCount, CanonicalChildrenWhiteList
    ).


%%--------------------------------------------------------------------
%% @equiv get_children_details_insecure/6 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children_details(user_ctx:ctx(), file_ctx:ctx(), file_listing:options()) ->
    fslogic_worker:fuse_response().
get_children_details(UserCtx, FileCtx0, ListOpts) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    get_children_details_insecure(UserCtx, FileCtx2, ListOpts, CanonicalChildrenWhiteList).


-spec list_recursively(user_ctx:ctx(), file_ctx:ctx(), recursive_file_listing:options()) ->
    fslogic_worker:provider_response().
list_recursively(UserCtx, FileCtx, ListOpts) ->
    #provider_response{status = #status{code = ?OK},
        provider_response = recursive_file_listing:list(UserCtx, FileCtx, ListOpts)
    }.
    

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% file_listing:list_children/4 with permissions_check
%% TODO VFS-7149 untangle permissions_check and fslogic_worker
%% @end
%%--------------------------------------------------------------------
-spec get_children_ctxs(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(), data_access_control:requirement()
) ->
    {
        ChildrenCtxs :: [file_ctx:ctx()],
        file_listing:pagination_token(),
        NewFileCtx :: file_ctx:ctx()
    }.
get_children_ctxs(UserCtx, FileCtx0, ListOpts, DirOperationsRequirements) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, DirOperationsRequirements];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    file_tree:list_children(FileCtx2, UserCtx, ListOpts, CanonicalChildrenWhiteList).


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
    File = file_meta:new_doc(Name, ?DIRECTORY_TYPE, Mode, Owner, ParentUuid, SpaceId),
    {ok, #document{key = DirUuid}} = file_meta:create({uuid, ParentUuid}, File),
    FileCtx = file_ctx:new_by_uuid(DirUuid, SpaceId),

    try
        {ok, Time} = times:save_with_current_times(DirUuid, SpaceId),
        dir_update_time_stats:report_update_of_dir(file_ctx:get_logical_guid_const(FileCtx), Time),
        fslogic_times:update_mtime_ctime(ParentFileCtx3),

        #fuse_response{fuse_response = FileAttr} =
            attr_req:get_file_attr_insecure(UserCtx, FileCtx, #{
                allow_deleted_files => false,
                include_size => false,
                name_conflicts_resolution_policy => allow_name_conflicts
            }),
        FileAttr2 = FileAttr#file_attr{size = 0},
        ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, [user_ctx:get_session_id(UserCtx)]),
        dir_size_stats:report_file_created(?DIRECTORY_TYPE, file_ctx:get_logical_guid_const(ParentFileCtx)),
        #fuse_response{status = #status{code = ?OK},
            fuse_response = #dir{guid = file_id:pack_guid(DirUuid, SpaceId)}
        }
    catch
        Error:Reason ->
            FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
            file_meta:delete(FileUuid),
            times:delete(FileUuid),
            erlang:Error(Reason)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th from specified Token entry and up to Limit of entries
%% and allowed by CanonicalChildrenWhiteList.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs_insecure(user_ctx:ctx(), file_ctx:ctx(), file_listing:options(),
    boolean(), boolean(), undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_attrs_insecure(
    UserCtx, FileCtx0, ListOpts, IncludeReplicationStatus, IncludeLinkCount, CanonicalChildrenWhiteList
) ->
    {Children, ListingToken, FileCtx1} = file_tree:list_children(
        FileCtx0, UserCtx, ListOpts, CanonicalChildrenWhiteList),
    ComputeAttrsOpts = #{
        allow_deleted_files => false,
        include_size => true,
        include_replication_status => IncludeReplicationStatus,
        include_link_count => IncludeLinkCount
    },
    ChildrenAttrs = readdir_plus:gather_attributes(
        UserCtx,
        child_attrs_mapper(fun attr_req:get_file_attr_insecure/3),
        Children,
        ComputeAttrsOpts
    ),

    fslogic_times:update_atime(FileCtx1),

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_attrs{
            child_attrs = ChildrenAttrs,
            pagination_token = ListingToken
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets file details (see file_details.hrl) for each directory children
%% starting with Offset-th from specified StartId entry and up to Limit
%% of entries and allowed by CanonicalChildrenWhiteList.
%% @end
%%--------------------------------------------------------------------
-spec get_children_details_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    file_listing:options(),
    undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_details_insecure(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList) ->
    file_ctx:is_user_root_dir_const(FileCtx0, UserCtx) andalso throw(?ENOTSUP),
    {Children, ListingToken, FileCtx1} = file_tree:list_children(
        FileCtx0, UserCtx, ListOpts, CanonicalChildrenWhiteList
    ),
    ComputeAttrsOpts = #{
        allow_deleted_files => false,
        include_size => true,
        include_replication_status => false,
        include_link_count => true
    },
    ChildrenDetails = readdir_plus:gather_attributes(
        UserCtx,
        child_attrs_mapper(fun attr_req:get_file_details_insecure/3),
        Children,
        ComputeAttrsOpts
    ),
    fslogic_times:update_atime(FileCtx1),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_details{
            child_details = ChildrenDetails,
            pagination_token = ListingToken
        }
    }.


%% @private
-spec child_attrs_mapper(map_child_fun()) -> 
    readdir_plus:gather_attributes_fun(file_ctx:ctx(), fslogic_worker:fuse_response_type()).
child_attrs_mapper(AttrsMappingFun) ->
    fun(UserCtx, ChildCtx, BaseOpts) ->
        #fuse_response{status = #status{code = ?OK}, fuse_response = Result} = 
            AttrsMappingFun(UserCtx, ChildCtx, BaseOpts),
        Result
    end.

