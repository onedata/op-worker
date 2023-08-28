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

-include("global_definitions.hrl").
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

-define(MAX_MAP_CHILDREN_PROCESSES, application:get_env(
    ?APP_NAME, max_read_dir_plus_procs, 20
)).

%%%===================================================================
%%% `recursive_listing` callbacks
%%%===================================================================

-spec is_branching_node(tree_node()) -> {boolean(), tree_node()} | not_found.
is_branching_node(FileCtx) ->
    ?safeguard_not_found(file_ctx:is_dir(FileCtx)).


-spec get_node_id(tree_node()) -> {node_id(), tree_node()}.
get_node_id(FileCtx) ->
    {file_ctx:get_logical_guid_const(FileCtx), FileCtx}.


-spec get_node_name(tree_node(), user_ctx:ctx() | undefined) -> {node_name(), tree_node()} | not_found.
get_node_name(FileCtx0, UserCtx) ->
    ?safeguard_not_found(file_ctx:get_aliased_name(FileCtx0, UserCtx)).


-spec get_node_path_tokens(tree_node()) -> {[node_name()], tree_node()} | not_found.
get_node_path_tokens(FileCtx) ->
    ?safeguard_not_found(begin
        {UuidPath, FileCtx1} = file_ctx:get_uuid_based_path(FileCtx),
        [_Separator, SpaceId | Uuids] = filename:split(UuidPath),
        {ok, SpaceName} = space_logic:get_name(?ROOT_SESS_ID, SpaceId),
        PathTokens = lists:map(fun(Uuid) ->
            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            TokenFileCtx = file_ctx:new_by_uuid(Uuid, SpaceId),
            case ?safeguard_not_found(file_attr:resolve(UserCtx, TokenFileCtx, #{attributes => [name]})) of
                not_found ->
                    throw(not_found);
                {#file_attr{name = Name}, _Ctx} ->
                    Name
            end
        end, Uuids),
        {[SpaceName | PathTokens], FileCtx1}
    end).


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
        {Children, PaginationToken, FileCtx2} = dir_req:get_children_ctxs(
            UserCtx, FileCtx, ListOpts
        ),
        ProgressMarker = case file_listing:is_finished(PaginationToken) of
            true -> done;
            false -> more
        end,
        {ProgressMarker, cache_file_doc_in_batch(Children), #{
            node => FileCtx2,
            opts => #{pagination_token => PaginationToken}}
        }
    catch throw:?EACCES ->
        no_access
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec cache_file_doc_in_batch([file_ctx:ctx()]) -> [file_ctx:ctx()].
cache_file_doc_in_batch(FileCtxs) ->
    FilterMapFun = fun(Ctx) ->
        ?safeguard_not_found(begin
            {_, Ctx2} = file_ctx:get_file_doc(Ctx),
            {true, Ctx2}
        end, false)
    end,
    lists_utils:pfiltermap(FilterMapFun, FileCtxs, ?MAX_MAP_CHILDREN_PROCESSES).
