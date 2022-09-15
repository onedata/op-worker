%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements `recursive_listing_node_behaviour` behaviour callbacks to allow 
%%% recursive listing of file tree structure. 
%%% Files are listed lexicographically ordered by path.
%%% For each such file returns its file basic attributes (see file_attr.hrl) 
%%% along with the path to the file relative to the top directory.
%%% All directory paths user does not have access to are returned under `inaccessible_paths` key.  
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_file_listing_node).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(recursive_listing_node_behaviour).

%% `recursive_listing_node_behaviour` callbacks
-export([
    is_branching_node/1,
    get_node_id/1, get_node_name/2, get_node_path_tokens/1,
    init_node_iterator/3,
    get_next_batch/2
]).


-type node_id() :: file_id:file_guid().
-type tree_node() :: file_ctx:ctx().
-type node_name() :: file_meta:name().
-type node_path() :: file_meta:path().
-type node_iterator() :: #{
    node := tree_node(),
    opts := file_listing:options()
}.
-type entry() :: recursive_listing:result_entry(node_path(), lfm_attrs:file_attributes()).
-type result() :: recursive_listing:result(node_path(), entry()).

-export_type([node_path/0, result/0, entry/0]).

%%%===================================================================
%%% `recursive_listing` callbacks
%%%===================================================================

-spec is_branching_node(tree_node()) -> {boolean(), tree_node()}.
is_branching_node(FileCtx) ->
    file_ctx:is_dir(FileCtx).


-spec get_node_id(tree_node()) -> {node_id(), tree_node()}.
get_node_id(FileCtx) ->
    {file_ctx:get_logical_guid_const(FileCtx), FileCtx}.


-spec get_node_name(tree_node(), user_ctx:ctx() | undefined) -> {node_name(), tree_node()}.
get_node_name(FileCtx0, UserCtx) ->
    {FileName, FileCtx1} = file_ctx:get_aliased_name(FileCtx0, UserCtx),
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx1),
    ProviderId = file_meta:get_provider_id(FileDoc),
    {ok, ParentUuid} = file_meta:get_parent_uuid(FileDoc),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx2),
    
    case file_meta:check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, ProviderId) of
        {conflicting, ExtendedName, _ConflictingFiles} ->
            {ExtendedName, FileCtx2};
        _ ->
            {FileName, FileCtx2}
    end.


-spec get_node_path_tokens(tree_node()) -> {[node_name()], tree_node()}.
get_node_path_tokens(FileCtx) ->
    {UuidPath, FileCtx1} = file_ctx:get_uuid_based_path(FileCtx),
    [_Separator, SpaceId | Uuids] = filename:split(UuidPath),
    {ok, SpaceName} = space_logic:get_name(?ROOT_SESS_ID, SpaceId),
    PathTokens = lists:map(fun(Uuid) ->
        {Name, _} = get_node_name(file_ctx:new_by_uuid(Uuid, SpaceId), user_ctx:new(?ROOT_SESS_ID)),
        Name
    end, Uuids),
    {[SpaceName | PathTokens], FileCtx1}.


-spec init_node_iterator(tree_node(), node_name() | undefined, recursive_listing:limit()) -> 
    node_iterator().
init_node_iterator(FileCtx, undefined, Limit) ->
    #{
        node => FileCtx,
        opts => #{limit => Limit, tune_for_large_continuous_listing => Limit =/= 1}
    };
init_node_iterator(FileCtx, StartFileName, Limit) ->
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    BaseOpts = #{limit => Limit, tune_for_large_continuous_listing => Limit =/= 1},
    ListingOpts = case file_meta:get_child_uuid_and_tree_id(Uuid, StartFileName) of
        {ok, _, TreeId} ->
            StartingIndex = file_listing:build_index(
                file_meta:trim_filename_tree_id(StartFileName, TreeId), TreeId),
            #{index => StartingIndex, inclusive => true};
        _ ->
            StartingIndex = file_listing:build_index(file_meta:trim_filename_tree_id(
                StartFileName, {all, Uuid})),
            #{index => StartingIndex}
    end,
    #{
        node => FileCtx,
        opts => maps:merge(BaseOpts, ListingOpts)
    }.


-spec get_next_batch(node_iterator(), user_ctx:ctx()) ->
    {more | done, [tree_node()], node_iterator()} | no_access.
get_next_batch(#{node := FileCtx, opts := ListOpts}, UserCtx) ->
    try
        {CanonicalChildrenWhiteList, FileCtx2} = case file_ctx:is_dir(FileCtx) of
            {true, Ctx} -> check_dir_access(UserCtx, Ctx);
            {false, Ctx} -> check_non_dir_access(UserCtx, Ctx)
        end,
        {Children, PaginationToken, FileCtx3} = file_tree:list_children(
            FileCtx2, UserCtx, ListOpts, CanonicalChildrenWhiteList
        ),
        ProgressMarker = case file_listing:is_finished(PaginationToken) of
            true -> done;
            false -> more
        end,
        {ProgressMarker, Children, #{
            node => FileCtx3, 
            opts => #{pagination_token => PaginationToken}}
        }
    catch throw:?EACCES ->
        no_access
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec check_dir_access(user_ctx:ctx(), file_ctx:ctx()) ->
    {undefined | [file_meta:name()], file_ctx:ctx()}.
check_dir_access(UserCtx, DirCtx) ->
    fslogic_authz:ensure_authorized_readdir(UserCtx, DirCtx, 
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)]).


%% @private
-spec check_non_dir_access(user_ctx:ctx(), file_ctx:ctx()) -> 
    {undefined | [file_meta:name()], file_ctx:ctx()}.
check_non_dir_access(UserCtx, FileCtx) ->
    fslogic_authz:ensure_authorized_readdir(UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]).
