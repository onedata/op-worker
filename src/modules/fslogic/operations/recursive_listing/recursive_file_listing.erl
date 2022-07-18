%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for listing recursively non-directory files 
%%% (i.e regular, symlinks and hardlinks) in subtree of given top directory. 
%%% Files are listed lexicographically ordered by path.
%%% For each such file returns its file basic attributes (see file_attr.hrl) 
%%% along with the path to the file relative to the top directory.
%%% All directory paths user does not have access to are returned under `inaccessible_paths` key. 
%%% 
%%% When options `include_directories` is set to true directory entries will be included in result.
%%% For other options description consult `recursive_listing` module doc.
%%% @end
%%%--------------------------------------------------------------------
-module(recursive_file_listing).
-author("Michal Stanisz").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

-behaviour(recursive_listing_behaviour).

%% API
-export([list/3]).

%% `recursive_listing_behaviour` callbacks
-export([
    is_traversable_object/1,
    get_object_id/1,  get_object_path/1, get_object_name/2, get_parent_id/2,
    build_listing_opts/4,
    check_access/2, list_children_with_access_check/3,
    is_listing_finished/1
]).


-type object_id() :: file_id:file_guid().
-type object() :: file_ctx:ctx().
-type name() :: file_meta:name().
-type path() :: file_meta:path().
-type object_listing_opts() :: file_listing:options().
-type entry() :: {file_meta:path(), lfm_attrs:file_attributes()}.
-type result() :: recursive_listing:record().

% For detailed options description see module doc.
-type options() :: #{
    % NOTE: pagination_token and start_after_path are mutually exclusive
    pagination_token => pagination_token(),
    start_after_path => path(),
    prefix => recursive_listing:prefix(),
    limit => recursive_listing:limit(),
    include_directories => boolean()
}.
-type pagination_token() :: recursive_listing:pagination_token().

-export_type([result/0, entry/0, options/0, pagination_token/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec list(user_ctx:ctx(), file_ctx:ctx(), options()) -> result().
list(UserCtx, FileCtx, ListOpts) ->
    FinalListOpts = maps:remove(
        include_directories, 
        maps_utils:put_if_defined(ListOpts, include_traversable, 
            maps:get(include_directories, ListOpts, undefined)
        )
    ),
    #recursive_listing_result{
        entries = Entries
    } = Result = recursive_listing:list(?MODULE, UserCtx, FileCtx, FinalListOpts),
    ComputeAttrsOpts = #{
        allow_deleted_files => false,
        include_size => true,
        include_replication_status => false,
        include_link_count => false
    },
    MappedEntries = readdir_plus:gather_attributes(
        UserCtx, fun map_result_entry/3, Entries, ComputeAttrsOpts),
    Result#recursive_listing_result{entries = MappedEntries}.


%%%===================================================================
%%% `recursive_listing_behaviour` callbacks
%%%===================================================================

-spec is_traversable_object(object()) -> {boolean(), object()}.
is_traversable_object(FileCtx) ->
    file_ctx:is_dir(FileCtx).


-spec get_object_id(object()) -> object_id().
get_object_id(FileCtx) ->
    file_ctx:get_logical_guid_const(FileCtx).


-spec get_object_name(object(), user_ctx:ctx() | undefined) -> {name(), object()}.
get_object_name(FileCtx, UserCtx) ->
    file_ctx:get_aliased_name(FileCtx, UserCtx).


-spec get_object_path(object()) -> {path(), object()}.
get_object_path(FileCtx) ->
    file_ctx:get_canonical_path(FileCtx).


-spec get_parent_id(object(), user_ctx:ctx()) -> object_id().
get_parent_id(FileCtx, UserCtx) ->
    {ParentCtx, _} = file_tree:get_parent(FileCtx, UserCtx),
    case file_ctx:equals(ParentCtx, FileCtx) of
        true -> <<>>;
        false -> file_ctx:get_logical_guid_const(ParentCtx)
    end.


-spec build_listing_opts(name(), recursive_listing:limit(), boolean(), object_id()) -> 
    object_listing_opts().
build_listing_opts(undefined, Limit, IsContinuous, _ParentGuid) ->
    #{limit => Limit, tune_for_large_continuous_listing => IsContinuous};
build_listing_opts(StartFileName, Limit, IsContinuous, ParentGuid) ->
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    BaseOpts = #{limit => Limit, tune_for_large_continuous_listing => IsContinuous},
    ListingOpts = case file_meta:get_child_uuid_and_tree_id(ParentUuid, StartFileName) of
        {ok, _, TreeId} ->
            StartingIndex = file_listing:build_index(
                file_meta:trim_filename_tree_id(StartFileName, TreeId), TreeId),
            #{index => StartingIndex, inclusive => true};
        _ ->
            StartingIndex = file_listing:build_index(file_meta:trim_filename_tree_id(
                StartFileName, {all, ParentUuid})),
            #{index => StartingIndex}
    end,
    maps:merge(BaseOpts, ListingOpts).


-spec check_access(object(), user_ctx:ctx()) -> ok | {error, ?EACCES}.
check_access(FileCtx, UserCtx) ->
    try
        {CanonicalChildrenWhiteList, FileCtx2} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, FileCtx, [?TRAVERSE_ANCESTORS]
        ),
        % throws on lack of access
        _ = file_tree:list_children(FileCtx2, UserCtx, #{
            limit => 1,
            tune_for_large_continuous_listing => false
        }, CanonicalChildrenWhiteList),
        ok
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


-spec list_children_with_access_check(object(), object_listing_opts(), user_ctx:ctx()) ->
    {ok, [object()], object_listing_opts(), object()} | {error, ?EACCES}.
list_children_with_access_check(DirCtx, ListOpts, UserCtx) ->
    try
        {CanonicalChildrenWhiteList, DirCtx2} = fslogic_authz:ensure_authorized_readdir(
            UserCtx, DirCtx, [?TRAVERSE_ANCESTORS, ?OPERATIONS(?traverse_container_mask, ?list_container_mask)]),
        {Children, PaginationToken, DirCtx3} = file_tree:list_children(
            DirCtx2, UserCtx, ListOpts, CanonicalChildrenWhiteList),
        {ok, Children, #{pagination_token => PaginationToken}, DirCtx3}
    catch throw:?EACCES ->
        {error, ?EACCES}
    end.


-spec is_listing_finished(object_listing_opts()) -> boolean().
is_listing_finished(#{pagination_token := PaginationToken}) ->
    file_listing:is_finished(PaginationToken).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec map_result_entry(user_ctx:ctx(), {path(), object()}, attr_req:compute_file_attr_opts()) -> 
    entry() | no_return().
map_result_entry(UserCtx, {Path, FileCtx}, BaseOpts) ->
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = FileAttrs
    } = attr_req:get_file_attr_insecure(UserCtx, FileCtx, BaseOpts),
    {Path, FileAttrs}.
