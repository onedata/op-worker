%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module operates on two types of tokens used for continuous listing of directory content: 
%%%     * api_list_token - used by higher-level modules and passed to the external clients as an 
%%%                        opaque string so that they can resume listing just after previously 
%%%                        listed batch (results paging). It encodes a datastore_token and additional
%%%                        information about last position in the file tree, in case the datastore
%%%                        token expires. Hence, this token does not expire and always guarantees
%%%                        correct listing resumption.
%%%                    
%%%     * datastore_list_token - token used internally by datastore, has limited TTL. After its expiration, 
%%%                              information about last position in the file tree can be used to determine
%%%                              the starting point for listing. This token offers the best performance 
%%%                              when resuming listing from a certain point.
%%% @TODO VFS-8980 currently it is possible to pass datastore_token from outside
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").


-type api_list_token() :: binary().
-type list_token() :: file_meta:list_token() | api_list_token().
-type token_type() :: api_list_token | datastore_list_token.

%% @TODO VFS-8980 Create opaque structure for list opts
% When api_list_token is provided, other starting point options (offset, last_tree, last_name) are ignored
-type list_opts() :: #{
    token := api_list_token(),
    size := file_meta:list_size()
} | file_meta:list_opts().


-export_type([list_token/0, list_opts/0]).

%% API
-export([
    mkdir/4,
    get_children_ctxs/3,
    get_children/3,
    get_children_attrs/5,
    get_children_details/3
]).

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).
-define(api_list_token_PREFIX, "api_list_token").

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



-spec get_children(user_ctx:ctx(), file_ctx:ctx(), list_opts()) ->
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
-spec get_children_ctxs(user_ctx:ctx(), file_ctx:ctx(), list_opts()
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
-spec get_children_attrs(user_ctx:ctx(), file_ctx:ctx(), list_opts(), boolean(), boolean()) ->
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
-spec get_children_details(user_ctx:ctx(), file_ctx:ctx(), list_opts()) ->
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
%% starting with Offset-th from specified Token entry and up to Limit of entries.
%% and allowed by CanonicalChildrenWhiteList.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs_insecure(user_ctx:ctx(), file_ctx:ctx(), list_opts(),
    boolean(), boolean(), undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_attrs_insecure(
    UserCtx, FileCtx0, ListOpts, IncludeReplicationStatus, IncludeLinkCount, CanonicalChildrenWhiteList
) ->
    {Children, ExtendedInfo, FileCtx1} = list_children(
        UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList),
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
    list_opts(),
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


%% @private
-spec list_children(user_ctx:ctx(), file_ctx:ctx(), list_opts(),
    CanonicalChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    {
        Children :: [file_ctx:ctx()],
        ExtendedInfo :: file_meta:list_extended_info(),
        NewFileCtx :: file_ctx:ctx()
    }.
list_children(UserCtx, FileCtx0, ListOpts, CanonicalChildrenWhiteList) ->
    {TokenType, FinalListOpts} = resolve_list_opts(ListOpts),
    {Children, ExtendedInfo, NewFileCtx} = files_tree:get_children(
        FileCtx0, UserCtx, FinalListOpts, CanonicalChildrenWhiteList),
    FinalExtendedInfo = case TokenType of
        api_list_token -> 
            #{
                is_last => maps:get(is_last, ExtendedInfo),
                token => pack_api_list_token(ExtendedInfo)
            };
        _ ->
            ExtendedInfo
    end,
    {Children, FinalExtendedInfo, NewFileCtx}.


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


%% @private
-spec resolve_list_opts(list_opts()) -> {token_type() | none, file_meta:list_opts()}.
resolve_list_opts(#{token := undefined} = ListOpts) ->
    {none, ListOpts};
resolve_list_opts(#{token := ?INITIAL_API_LS_TOKEN} = ListOpts) ->
    {api_list_token, #{
        token => ?INITIAL_DATASTORE_LS_TOKEN,
        last_tree => <<>>,
        last_name => <<>>,
        size => maps:get(size, ListOpts)
    }};
resolve_list_opts(#{token := Token} = ListOpts) ->
    try
        {api_list_token, maps:merge(unpack_api_list_token(Token), #{
            size => maps:get(size, ListOpts)
        })}
    catch _:_ ->
        {datastore_list_token, ListOpts}
    end;
resolve_list_opts(ListOpts) ->
    {none, ListOpts}.


%% @private
-spec pack_api_list_token(file_meta:list_extended_info()) -> binary().
pack_api_list_token(#{token := DatastoreToken, last_tree := LastTree, last_name := LastName}) ->
    mochiweb_base64url:encode(str_utils:join_binary(
        [<<?api_list_token_PREFIX>>, DatastoreToken, LastTree, LastName], <<"#">>));
pack_api_list_token(_) ->
    undefined.


%% @private
-spec unpack_api_list_token(api_list_token()) -> map().
unpack_api_list_token(Token) ->
    <<?api_list_token_PREFIX, _/binary>> = DecodedToken = mochiweb_base64url:decode(Token),
    [_Header, DecodedDatastoreToken, LastTree | LastNameTokens] =
        binary:split(DecodedToken, <<"#">>, [global]),
    #{
        token => DecodedDatastoreToken,
        last_tree => LastTree,
        last_name => str_utils:join_binary(LastNameTokens, <<"#">>)
    }.