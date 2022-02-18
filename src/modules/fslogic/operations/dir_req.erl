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
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([
    mkdir/4,
    get_children_ctxs/3,
    get_children/3,
    get_children_attrs/5,
    get_children_details/3,
    get_recursive_file_list/4
]).

%% @TODO VFS-9051 - Refactor recursive file listing

-record(list_recursive_state, {
    % Split relative path between start after and start file path.
    % Each filename in the list is a starting point at corresponding subtree level, 
    % the only exception being the last token, if it matches regular file - then it should be filtered out.
    filters :: [file_meta:name()],
    limit :: non_neg_integer(),
    current_path_tokens :: [file_meta:name()],
    previous_filter :: undefined | file_meta:name(),
    parent_uuid :: file_meta:uuid(),
    canonical_children_whitelist :: undefined | [file_meta:path()]
}).

-type list_recursive_state() :: #list_recursive_state{}.

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).
-define(LIST_RECURSIVE_BATCH_SIZE, 1000).

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



-spec get_children(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts()) ->
    fslogic_worker:fuse_response().
get_children(UserCtx, FileCtx0, ListOpts) ->
    ParentGuid = file_ctx:get_logical_guid_const(FileCtx0),
    {ChildrenCtxs, ExtendedInfo, FileCtx1} = get_children_ctxs(UserCtx, FileCtx0, ListOpts),
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
                    case file_meta:check_name_and_get_conflicting_files(file_id:guid_to_uuid(ParentGuid), ChildName, FileUuid, ProviderId) of
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
            index_token = maps:get(token, ExtendedInfo, undefined),
            is_last = maps:get(is_last, ExtendedInfo)
        }
    }.


%%--------------------------------------------------------------------
%% @doc
%% list_children/8 with permissions_check
%% TODO VFS-7149 untangle permissions_check and fslogic_worker
%% @end
%%--------------------------------------------------------------------
-spec get_children_ctxs(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts()
) ->
    {
        ChildrenCtxs :: [file_ctx:ctx()],
        ExtendedListInfo :: file_meta:list_extended_info(),
        NewFileCtx :: file_ctx:ctx()
    }.
get_children_ctxs(UserCtx, FileCtx0, ListOpts) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?list_container_mask)];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    list_children(UserCtx, FileCtx2, ListOpts, CanonicalChildrenWhiteList).


%%--------------------------------------------------------------------
%% @equiv get_children_attrs_insecure/7 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts(), boolean(), boolean()) ->
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
-spec get_children_details(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts()) ->
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


%%--------------------------------------------------------------------
%% @equiv list_recusrive_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_recursive_file_list(user_ctx:ctx(), file_ctx:ctx(), file_meta:path(), non_neg_integer()) ->
    fslogic_worker:fuse_response().
get_recursive_file_list(UserCtx, FileCtx0, StartAfter, Limit) ->
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    AccessRequirements = case IsDir of
        true -> [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)];
        false -> [?TRAVERSE_ANCESTORS]
    end,
    {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx1, AccessRequirements
    ),
    list_recursive_insecure(UserCtx, FileCtx2, StartAfter, Limit, CanonicalChildrenWhiteList).


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
-spec get_children_attrs_insecure(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts(),
    boolean(), boolean(), undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_attrs_insecure(
    UserCtx, FileCtx0, ListOpts, IncludeReplicationStatus, IncludeLinkCount, CanonicalChildrenWhiteList
) ->
    {Children, ExtendedInfo, FileCtx1} = list_children(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList),
    ChildrenAttrs = map_children(
        UserCtx,
        fun attr_req:get_file_attr_insecure/3,
        Children,
        IncludeReplicationStatus,
        IncludeLinkCount
    ),

    fslogic_times:update_atime(FileCtx1),

    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_attrs{
            child_attrs = ChildrenAttrs,
            index_token = maps:get(token, ExtendedInfo, undefined),
            is_last = maps:get(is_last, ExtendedInfo)
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
    file_meta:list_opts(),
    undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_details_insecure(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList) ->
    file_ctx:is_user_root_dir_const(FileCtx0, UserCtx) andalso throw(?ENOTSUP),
    {Children, ListExtendedInfo, FileCtx1} = list_children(
        UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList
    ),
    ChildrenDetails = map_children(
        UserCtx,
        fun attr_req:get_file_details_insecure/3,
        Children,
        false,
        true
    ),
    fslogic_times:update_atime(FileCtx1),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_details{
            child_details = ChildrenDetails,
            is_last = maps:get(is_last, ListExtendedInfo)
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each regular file 
%% that is in a subtree of given file starting after path from specified 
%% StartAfter entry and up to Limit of entries and allowed by CanonicalChildrenWhiteList.
%% @end
%%--------------------------------------------------------------------
-spec list_recursive_insecure(user_ctx:ctx(), file_ctx:ctx(), file_meta:path(), non_neg_integer(), 
    undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
list_recursive_insecure(UserCtx, FileCtx0, StartAfter, Limit, CanonicalChildrenWhiteList) ->
    {StartPath, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    [_Sep, SpaceId | StartPathTokens] = filename:split(StartPath),
    case prepare_recursive_list_filters(
        filename:join(SpaceId, StartAfter), filename:join([SpaceId | StartPathTokens])
    ) of
        {cont, Filters} ->
            {Res, IsLast} = list_recursive_next_file(UserCtx, FileCtx1, #list_recursive_state{
                filters = Filters,
                limit = Limit,
                current_path_tokens = [SpaceId | StartPathTokens],
                previous_filter = filename:basename(StartPath),
                parent_uuid = file_ctx:get_logical_uuid_const(FileCtx1),
                canonical_children_whitelist = CanonicalChildrenWhiteList
            }),
            
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #recursive_file_list{
                    files = Res,
                    is_last = IsLast
                }
            };
        halt ->
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #recursive_file_list{
                    files = [],
                    is_last = true
                }
            }
    end.


%% @private
-spec list_children(user_ctx:ctx(), file_ctx:ctx(), file_meta:list_opts(),
    CanonicalChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    {
        Children :: [file_ctx:ctx()],
        ExtendedInfo :: file_meta:list_extended_info(),
        NewFileCtx :: file_ctx:ctx()
    }.
list_children(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList) ->
    files_tree:get_children(FileCtx0, UserCtx, ListOpts, CanonicalChildrenWhiteList).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls MapFunctionInsecure for every passed children in parallel and
%% filters out children for which it raised error (potentially docs not
%% synchronized between providers or deleted files).
%% @end
%%--------------------------------------------------------------------
-spec map_children(
    UserCtx,
    MapFunInsecure :: fun((UserCtx, ChildCtx :: file_ctx:ctx(), attr_req:compute_file_attr_opts()) ->
        fslogic_worker:fuse_response()),
    Children :: [file_ctx:ctx()],
    IncludeReplicationStatus :: boolean(),
    IncludeLinkCount :: boolean()
) ->
    [fuse_response_type()] when UserCtx :: user_ctx:ctx().
map_children(UserCtx, MapFunInsecure, Children, IncludeReplicationStatus, IncludeLinkCount) ->
    ChildrenNum = length(Children),
    EnumeratedChildren = lists_utils:enumerate(Children),
    ComputeFileAttrOpts = #{
        allow_deleted_files => false,
        include_size => true
    },
    FilterMapFun = fun({Num, ChildCtx}) ->
        try
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = Result
            } = case Num == 1 orelse Num == ChildrenNum of
                true ->
                    MapFunInsecure(UserCtx, ChildCtx, ComputeFileAttrOpts#{
                        name_conflicts_resolution_policy => resolve_name_conflicts,
                        include_replication_status => IncludeReplicationStatus,
                        include_link_count => IncludeLinkCount
                    });
                false ->
                    % Other files than first and last don't need to resolve name
                    % conflicts (to check for collisions) as list_children
                    % (file_meta:tag_children to be precise) already did it
                    MapFunInsecure(UserCtx, ChildCtx, ComputeFileAttrOpts#{
                        name_conflicts_resolution_policy => allow_name_conflicts,
                        include_replication_status => IncludeReplicationStatus,
                        include_link_count => IncludeLinkCount
                    })
            end,
            {true, Result}
        catch _:_ ->
            % File can be not synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, EnumeratedChildren, ?MAX_MAP_CHILDREN_PROCESSES).


-spec prepare_recursive_list_filters(file_meta:path(), file_meta:path()) -> 
    {cont, [file_meta:name()]} | halt.
prepare_recursive_list_filters(StartAfter, StartPath) when StartAfter =< StartPath ->
    {cont, []};
prepare_recursive_list_filters(StartAfter, StartPath) ->
    case filepath_utils:is_descendant(StartAfter, StartPath) of
        false ->
            halt;
        {true, RelPath} ->
            {cont, filename:split(RelPath)}
    end.


-spec list_recursive_next_file(user_ctx:ctx(), file_ctx:ctx(), list_recursive_state()) ->
    {[{file_meta:path(), lfm_attrs:file_attributes()}], boolean()}.
list_recursive_next_file(_UserCtx, _FileCtx, #list_recursive_state{limit = Limit}) when Limit =< 0 ->
    {[], false};
list_recursive_next_file(UserCtx, FileCtx, State) ->
    {ListOpts, UpdatedState} = build_starting_list_opts(State),
    list_recursive_children_next_batch(UserCtx, FileCtx, ListOpts, UpdatedState, []).


-spec list_recursive_children_next_batch(user_ctx:ctx(), file_ctx:ctx(), map(), 
    list_recursive_state(), [{file_meta:path(), lfm_attrs:file_attributes()}]) ->
    {[{file_meta:path(), lfm_attrs:file_attributes()}], boolean()}.
list_recursive_children_next_batch(UserCtx, FileCtx, ListOpts, State, Acc) ->
    #list_recursive_state{
        limit = Limit, 
        canonical_children_whitelist = CanonicalChildrenWhitelist
    } = State,
    {Children, #{is_last := IsLast} = ExtendedInfo, _FileCtx1} =
        list_children(UserCtx, FileCtx, ListOpts#{size => ?LIST_RECURSIVE_BATCH_SIZE}, CanonicalChildrenWhitelist),
    {Res, FinalProcessedFiles} = lists_utils:foldl_while(fun(ChildCtx, {TmpBatch, ProcessedFiles}) ->
        {ChildBatch, IsChildProcessed} = map_list_recursive_child(
            UserCtx, ChildCtx, file_ctx:get_logical_guid_const(FileCtx), State#list_recursive_state{
                limit = Limit - length(TmpBatch)
            }),
        BatchToReturn = TmpBatch ++ ChildBatch,
        FileProcessedIncrement = case IsChildProcessed of
            true -> 1;
            false -> 0
        end,
        ResToReturn = {BatchToReturn, ProcessedFiles + FileProcessedIncrement},
        case length(BatchToReturn) >= Limit of
            true -> {halt, ResToReturn};
            false -> {cont, ResToReturn}
        end
    end, {Acc, 0}, Children),
    case {length(Res) >= Limit, IsLast} of
        {true, _} ->
            {Res, IsLast and (FinalProcessedFiles == length(Children))};
        {false, true} ->
            {Res, true};
        {false, false} ->
            NextListOpts = maps:with([last_tree, last_name], ExtendedInfo),
            list_recursive_children_next_batch(
                UserCtx, FileCtx, NextListOpts, State#list_recursive_state{limit = Limit - length(Res)}, Res)
    end.


-spec map_list_recursive_child(user_ctx:ctx(), file_ctx:ctx(), file_id:file_guid(), list_recursive_state()) ->
    {[{file_meta:path(), lfm_attrs:file_attributes()}], boolean()}.
map_list_recursive_child(UserCtx, ChildCtx, ListedFileGuid, State) ->
    #list_recursive_state{
        current_path_tokens = CurrentPathTokens,
        previous_filter = LastFilter
    } = State,
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttrs
    } = attr_req:get_file_attr_insecure(UserCtx, ChildCtx, #{
        name_conflicts_resolution_policy => resolve_name_conflicts
    }),
    case FileAttrs of
        #file_attr{type = ?DIRECTORY_TYPE, name = Name, guid = G} ->
            {NextChildren, SubtreeFinished} = list_recursive_next_file(UserCtx, ChildCtx,
                State#list_recursive_state{
                    current_path_tokens = CurrentPathTokens ++ [Name],
                    parent_uuid = file_id:guid_to_uuid(G)
                }
            ),
            {NextChildren, SubtreeFinished};
        #file_attr{type = _, name = LastFilter} ->
            {[], true};
        #file_attr{guid = G} when G == ListedFileGuid ->
            % listing regular file should return this file
            [_SpaceId | RestPathTokens] = CurrentPathTokens,
            {[{filename:join(RestPathTokens), FileAttrs}], true};
        #file_attr{type = _, name = Name} ->
            [_SpaceId | RestPathTokens] = CurrentPathTokens,
            {[{filename:join(RestPathTokens ++ [Name]), FileAttrs}], true}
    end.


-spec build_starting_list_opts(list_recursive_state()) -> 
    {map(), list_recursive_state()}.
build_starting_list_opts(#list_recursive_state{previous_filter = undefined} = State) ->
    {#{last_name => <<>>}, State#list_recursive_state{filters = []}};
build_starting_list_opts(#list_recursive_state{filters = []} = State) ->
    {#{last_name => <<>>}, State#list_recursive_state{previous_filter = undefined}};
build_starting_list_opts(#list_recursive_state{
    filters = [Filter | NextFilters],
    previous_filter = PrevFilter,
    current_path_tokens = CurrentPathTokens,
    parent_uuid = ParentUuid
} = State) ->
    case lists:last(CurrentPathTokens) == PrevFilter of
        true ->
            Opts = case file_meta:get_child_uuid_and_tree_id(ParentUuid, Filter) of
                {ok, _, TreeId} ->
                    #{
                        % trim tree id to always have inclusive listing
                        last_tree => binary:part(TreeId, 0, size(TreeId) - 1),
                        last_name => file_meta:trim_filename_tree_id(Filter, TreeId)
                    };
                _ ->
                    #{
                        last_name => file_meta:trim_filename_tree_id(Filter, {all, ParentUuid})
                    }
            end,
            {Opts, State#list_recursive_state{filters = NextFilters, previous_filter = Filter}};
        _ ->
            % we are no longer in a subtree that is filtered, so all filters ca be dropped
            {
                #{last_name => <<>>}, 
                State#list_recursive_state{filters = [], previous_filter = undefined}
            }
    end.
