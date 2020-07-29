%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles operations on data constraints (e.g. verifying
%%% whether access to file should be allowed).
%%% There are 3 types of constraints:
%%% - allowed_paths    - list of paths which are allowed (also their subpaths).
%%%                      To verify if allowed_paths hold for some file one must
%%%                      check if file path is contained in list or is subpath
%%%                      of any path from list,
%%% - guid_constraints - list of guid whitelists. To verify if guid_constraints
%%%                      hold for some file one must check if every whitelist
%%%                      contains either that file's guid or any
%%%                      of its ancestors,
%%% - readonly mode    - flag telling whether only operations available in
%%%                      readonly mode can be performed.
%%% NOTE !!!
%%% Sometimes access may be granted not only to paths or subpaths allowed
%%% by constraints directly but also to those paths ancestors (operations like
%%% readdir, stat, resolve_path/guid, get_parent). For such cases whitelist
%%% containing file's immediate children leading to paths allowed by
%%% constraints is also returned (may be useful to e.g. filter
%%% readdir result).
%%%
%%% To see examples of how function is this module works please check
%%% data_constraints_test.erl
%%% @end
%%%-------------------------------------------------------------------
-module(data_constraints).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_allow_all_constraints/0, get/1, assert_not_readonly_mode/1, inspect/4, assert_not_readonly_target/2]).

-ifdef(TEST).
-export([
    intersect_path_whitelists/2,
    consolidate_paths/1,

    check_against_allowed_paths/2,
    is_ancestor/3, is_subpath/3, is_path_or_subpath/2
]).
-endif.


-type relation() :: subpath | {ancestor, ordsets:ordset(file_meta:name())}.

-type ancestor_policy() :: allow_ancestors | disallow_ancestors.

-type path_whitelist() :: [file_meta:path()].
-type allowed_paths() :: any | path_whitelist().

-type guid_whitelist() :: [file_id:file_guid()].
-type guid_constraints() :: any | [guid_whitelist()].

-record(constraints, {
    paths :: allowed_paths(),
    guids :: guid_constraints(),
    readonly = false :: boolean()
}).

-opaque constraints() :: #constraints{}.

-export_type([constraints/0, ancestor_policy/0]).


-define(CV_READONLY, #cv_data_readonly{}).
-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_allow_all_constraints() -> constraints().
get_allow_all_constraints() ->
    #constraints{paths = any, guids = any, readonly = false}.


-spec get([caveats:caveat()]) ->
    {ok, constraints()} | {error, invalid_constraints}.
get(Caveats) ->
    DataConstraints = lists:foldl(fun
        (?CV_READONLY, Acc) ->
            Acc#constraints{readonly = true};
        (?CV_PATH(Paths), #constraints{paths = any} = Acc) ->
            Acc#constraints{paths = consolidate_paths(Paths)};
        (?CV_PATH(_Paths), #constraints{paths = []} = Acc) ->
            Acc;
        (?CV_PATH(Paths), #constraints{paths = AllowedPaths} = Acc) ->
            Acc#constraints{paths = intersect_path_whitelists(
                consolidate_paths(Paths), AllowedPaths
            )};
        (?CV_OBJECTID(ObjectIds), #constraints{guids = any} = Acc) ->
            Acc#constraints{
                guids = case objectids_to_guid_whitelist(ObjectIds) of
                    [] -> [];
                    GuidWhiteList -> [GuidWhiteList]
                end
            };
        (?CV_OBJECTID(ObjectIds), #constraints{guids = GuidConstraints} = Acc) ->
            Acc#constraints{
                guids = case objectids_to_guid_whitelist(ObjectIds) of
                    [] -> GuidConstraints;
                    GuidWhiteList -> [GuidWhiteList | GuidConstraints]
                end
            };
        (_, Acc) ->
            Acc
    end, #constraints{paths = any, guids = any}, Caveats),

    case DataConstraints of
        #constraints{paths = []} ->
            {error, invalid_constraints};
        #constraints{guids = []} ->
            {error, invalid_constraints};
        _ ->
            {ok, DataConstraints}
    end.


-spec assert_not_readonly_mode(user_ctx:ctx()) -> ok | no_return().
assert_not_readonly_mode(UserCtx) ->
    DataConstraints = user_ctx:get_data_constraints(UserCtx),
    case DataConstraints#constraints.readonly of
        true -> throw(?EACCES);
        false -> ok
    end.


-spec assert_not_readonly_target(od_provider:id(), od_space:id()) -> ok | no_return().
assert_not_readonly_target(ProviderId, SpaceId) ->
    case space_logic:has_readonly_support_from(SpaceId, ProviderId) of
        true -> throw(?EROFS);
        false -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies data constraints, that is whether access to specified file
%% can be granted. AncestorPolicy tells whether access can be granted
%% not only for files/directories directly allowed by constraints but also
%% to their ancestors.
%% For some operations and in case of file being ancestor to paths allowed
%% by constraints (with AllowAncestorsOfPaths set to allow_ancestors) it may
%% be necessary to know list of file's immediate children leading to paths
%% allowed by constraints. That is why it is also returned.
%% NOTE !!!
%% AllowAncestorsOfPaths set to allow_ancestors involves potentially higher
%% calculation cost so it should be used only in necessity.
%% @end
%%--------------------------------------------------------------------
-spec inspect(
    UserCtx :: user_ctx:ctx(),
    FileCtx :: file_ctx:ctx(),
    AncestorPolicy :: ancestor_policy(),
    AccessRequirements :: [data_access_rights:requirement()]
) ->
    {ChildrenWhiteList :: undefined | [file_meta:name()], file_ctx:ctx()}.
inspect(UserCtx, FileCtx0, AncestorPolicy, AccessRequirements) ->
    DataConstraints = user_ctx:get_data_constraints(UserCtx),

    case DataConstraints#constraints.readonly of
        true ->
            data_access_rights:assert_operation_available_in_readonly_mode(
                AccessRequirements
            );
        false ->
            ok
    end,
    case DataConstraints of
        #constraints{paths = any, guids = any} ->
            {undefined, FileCtx0};
        _ ->
            CheckResult = check_and_cache_data_constraints(
                UserCtx, FileCtx0, DataConstraints, AncestorPolicy
            ),
            case CheckResult of
                {subpath, FileCtx1} ->
                    {undefined, FileCtx1};
                {{ancestor, ChildrenWhiteList}, FileCtx1} ->
                    {ChildrenWhiteList, FileCtx1}
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Intersects 2 path whitelists.
%% NOTE !!!
%% Those whitelists must be consolidated before calling this function.
%% @end
%%--------------------------------------------------------------------
-spec intersect_path_whitelists(path_whitelist(), path_whitelist()) ->
    path_whitelist().
intersect_path_whitelists(WhiteListA, WhiteListB) ->
    intersect_path_whitelists(WhiteListA, WhiteListB, []).


%% @private
-spec intersect_path_whitelists(
    WhiteListA :: path_whitelist(),
    WhiteListB :: path_whitelist(),
    Intersection :: path_whitelist()
) ->
    UpdatedIntersection :: path_whitelist().
intersect_path_whitelists([], _, Intersection) ->
    lists:reverse(Intersection);
intersect_path_whitelists(_, [], Intersection) ->
    lists:reverse(Intersection);
intersect_path_whitelists(
    [PathA | RestA] = WhiteListA,
    [PathB | RestB] = WhiteListB,
    Intersection
) ->
    PathALen = size(PathA),
    PathBLen = size(PathB),

    case PathA < PathB of
        true ->
            case is_subpath(PathB, PathA, PathALen) of
                true ->
                    intersect_path_whitelists(RestA, RestB, [PathB | Intersection]);
                false ->
                    intersect_path_whitelists(RestA, WhiteListB, Intersection)
            end;
        false ->
            case is_path_or_subpath(PathA, PathB, PathBLen) of
                true ->
                    intersect_path_whitelists(RestA, RestB, [PathA | Intersection]);
                false ->
                    intersect_path_whitelists(WhiteListA, RestB, Intersection)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes trailing '/' from paths and consolidates them by removing
%% ones that are subpaths of others (e.g consolidation of
%% [/a/b/, /a/b/c, /q/w/e] results in [/a/b, /q/w/e]).
%% @end
%%--------------------------------------------------------------------
-spec consolidate_paths(path_whitelist()) -> path_whitelist().
consolidate_paths(Paths) ->
    TrimmedPaths = [string:trim(Path, trailing, "/") || Path <- Paths],
    consolidate_paths(lists:usort(TrimmedPaths), []).


%% @private
-spec consolidate_paths(
    Paths :: path_whitelist(),
    ConsolidatedPaths :: path_whitelist()
) ->
    UpdatedConsolidatedPaths :: path_whitelist().
consolidate_paths([], ConsolidatedPaths) ->
    lists:reverse(ConsolidatedPaths);
consolidate_paths([Path], ConsolidatedPaths) ->
    lists:reverse([Path | ConsolidatedPaths]);
consolidate_paths([PathA, PathB | RestOfPaths], ConsolidatedPaths) ->
    case is_path_or_subpath(PathB, PathA) of
        true ->
            consolidate_paths([PathA | RestOfPaths], ConsolidatedPaths);
        false ->
            consolidate_paths([PathB | RestOfPaths], [PathA | ConsolidatedPaths])
    end.


%% @private
-spec check_and_cache_data_constraints(user_ctx:ctx(), file_ctx:ctx(),
    constraints(), ancestor_policy()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_and_cache_data_constraints(UserCtx, FileCtx0, #constraints{
    paths = AllowedPaths,
    guids = GuidConstraints
}, AncestorPolicy) ->
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    SerializedToken = get_access_token(UserCtx),
    CacheKey = {data_constraint, SerializedToken, FileGuid},
    case permissions_cache:check_permission(CacheKey) of
        {ok, subpath} ->
            % File is allowed by constraints - every operations are permitted
            {subpath, FileCtx0};
        {ok, {subpath, ?EACCES}} when AncestorPolicy =:= disallow_ancestors ->
            % File is not allowed by constraints but it may be ancestor to some
            % of those paths (such checks were not performed) - forbid only
            % operations on subpaths
            throw(?EACCES);
        {ok, {ancestor, _} = Ancestor} ->
            % File is ancestor to path allowed by constraints so only specific
            % operations are allowed
            case AncestorPolicy of
                allow_ancestors -> {Ancestor, FileCtx0};
                disallow_ancestors -> throw(?EACCES)
            end;
        {ok, ?EACCES} ->
            % File is not permitted by constraints - eacces
            throw(?EACCES);
        _ ->
            try
                {PathRel, FileCtx1} = check_allowed_paths(
                    FileCtx0, AllowedPaths, AncestorPolicy
                ),
                {GuidRel, FileCtx2} = check_guid_constraints(
                    UserCtx, SerializedToken, FileCtx1,
                    GuidConstraints, AncestorPolicy
                ),
                Result = intersect_relations(PathRel, GuidRel),
                permissions_cache:cache_permission(CacheKey, Result),
                {Result, FileCtx2}
            catch throw:?EACCES ->
                case AncestorPolicy of
                    allow_ancestors ->
                        permissions_cache:cache_permission(CacheKey, ?EACCES);
                    disallow_ancestors ->
                        permissions_cache:cache_permission(
                            CacheKey, {subpath, ?EACCES}
                        )
                end,
                throw(?EACCES)
            end
    end.


%% @private
-spec check_allowed_paths(file_ctx:ctx(), allowed_paths(), ancestor_policy()) ->
    {relation(), file_ctx:ctx()} | no_return().
check_allowed_paths(FileCtx, any, _AncestorPolicy) ->
    % 'any' allows all subpaths of "/" - effectively all files
    {subpath, FileCtx};
check_allowed_paths(FileCtx0, AllowedPaths, disallow_ancestors) ->
    {FilePath, FileCtx1} = get_canonical_path(FileCtx0),

    IsFileAllowedSubPath = lists:any(fun(AllowedPath) ->
        is_path_or_subpath(FilePath, AllowedPath)
    end, AllowedPaths),

    case IsFileAllowedSubPath of
        true ->
            {subpath, FileCtx1};
        false ->
            throw(?EACCES)
    end;
check_allowed_paths(FileCtx0, AllowedPaths, allow_ancestors) ->
    {FilePath, FileCtx1} = get_canonical_path(FileCtx0),

    case check_against_allowed_paths(FilePath, AllowedPaths) of
        undefined ->
            throw(?EACCES);
        subpath ->
            {subpath, FileCtx1};
        {ancestor, _Children} = Ancestor ->
            {Ancestor, FileCtx1}
    end.


%% @private
-spec check_guid_constraints(user_ctx:ctx(), tokens:serialized(),
    file_ctx:ctx(), guid_constraints(), ancestor_policy()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_guid_constraints(_, _, FileCtx, any, _AncestorPolicy) ->
    % 'any' allows all descendants of UserRootDir ("/") - effectively all files
    {subpath, FileCtx};
check_guid_constraints(
    UserCtx, SerializedToken, FileCtx0, GuidConstraints, disallow_ancestors
) ->
    case does_fulfill_guid_constraints(
        UserCtx, SerializedToken, FileCtx0, GuidConstraints
    ) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, _} ->
            throw(?EACCES)
    end;
check_guid_constraints(
    UserCtx, SerializedToken, FileCtx0, GuidConstraints, allow_ancestors
) ->
    case does_fulfill_guid_constraints(
        UserCtx, SerializedToken, FileCtx0, GuidConstraints
    ) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, FileCtx1} ->
            {FilePath, FileCtx2} = get_canonical_path(FileCtx1),

            Relation = lists:foldl(fun(GuidsList, CurrRelation) ->
                AllowedPaths = guids_to_paths(GuidsList),
                case check_against_allowed_paths(FilePath, AllowedPaths) of
                    undefined ->
                        throw(?EACCES);
                    Rel ->
                        intersect_relations(CurrRelation, Rel)
                end
            end, subpath, GuidConstraints),

            {Relation, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether file fulfills guid constraints, which means that all
%% guid lists (contained in constraints) have either this file's guid or
%% any of it's ancestors (which is done recursively).
%% @end
%%--------------------------------------------------------------------
-spec does_fulfill_guid_constraints(user_ctx:ctx(), tokens:serialized(),
    file_ctx:ctx(), guid_constraints()
) ->
    {true, file_ctx:ctx()} |
    {false, guid_constraints(), file_ctx:ctx()}.
does_fulfill_guid_constraints(
    UserCtx, SerializedToken, FileCtx, AllGuidConstraints
) ->
    FileGuid = get_file_guid(FileCtx),
    CacheKey = {guid_constraint, SerializedToken, FileGuid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, true} ->
            {true, FileCtx};
        {ok, {false, NotFulfilledGuidConstraints}} ->
            {false, NotFulfilledGuidConstraints, FileCtx};
        _ ->
            case file_ctx:is_root_dir_const(FileCtx) of
                true ->
                    check_and_cache_guid_constraints_fulfillment(
                        FileCtx, CacheKey, AllGuidConstraints
                    );
                false ->
                    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx, UserCtx),
                    DoesParentFulfillGuidConstraints = does_fulfill_guid_constraints(
                        UserCtx, SerializedToken, ParentCtx,
                        AllGuidConstraints
                    ),
                    case DoesParentFulfillGuidConstraints of
                        {true, _} ->
                            permissions_cache:cache_permission(CacheKey, true),
                            {true, FileCtx1};
                        {false, RemainingGuidsConstraints, _} ->
                            check_and_cache_guid_constraints_fulfillment(
                                FileCtx, CacheKey, RemainingGuidsConstraints
                            )
                    end
            end
    end.


%% @private
-spec check_and_cache_guid_constraints_fulfillment(file_ctx:ctx(),
    CacheKey :: term(), guid_constraints()
) ->
    {true, file_ctx:ctx()} |
    {false, guid_constraints(), file_ctx:ctx()}.
check_and_cache_guid_constraints_fulfillment(FileCtx, CacheKey, GuidConstraints) ->
    FileGuid = get_file_guid(FileCtx),
    RemainingGuidConstraints = lists:filter(fun(GuidsList) ->
        not lists:member(FileGuid, GuidsList)
    end, GuidConstraints),

    case RemainingGuidConstraints of
        [] ->
            permissions_cache:cache_permission(CacheKey, true),
            {true, FileCtx};
        _ ->
            permissions_cache:cache_permission(
                CacheKey, {false, RemainingGuidConstraints}
            ),
            {false, RemainingGuidConstraints, FileCtx}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether Path is ancestor or subpath to any of specified
%% AllowedPaths. In case when Path is ancestor to one path and subpath
%% to another, then subpath takes precedence.
%% Additionally, if it is ancestor, it returns whitelist of it's immediate
%% children from AllowedPaths.
%% @end
%%--------------------------------------------------------------------
-spec check_against_allowed_paths(file_meta:path(), path_whitelist()) ->
    undefined | relation().
check_against_allowed_paths(Path, AllowedPaths) ->
    PathLen = size(Path),

    lists:foldl(fun
        (_AllowedPath, subpath) ->
            subpath;
        (AllowedPath, Acc) ->
            AllowedPathLen = size(AllowedPath),
            case PathLen >= AllowedPathLen of
                true ->
                    case is_path_or_subpath(Path, AllowedPath, AllowedPathLen) of
                        true -> subpath;
                        false -> Acc
                    end;
                false ->
                    case is_ancestor(Path, PathLen, AllowedPath) of
                        {true, Child} ->
                            NamesAcc = case Acc of
                                undefined -> ordsets:new();
                                {ancestor, Children} -> Children
                            end,
                            {ancestor, ordsets:add_element(Child, NamesAcc)};
                        false -> Acc
                    end
            end
    end, undefined, AllowedPaths).


%% @private
-spec intersect_relations(relation(), relation()) -> relation().
intersect_relations(subpath, subpath) ->
    subpath;
intersect_relations({ancestor, _Children} = Ancestor, subpath) ->
    Ancestor;
intersect_relations(subpath, {ancestor, _Children} = Ancestor) ->
    Ancestor;
intersect_relations({ancestor, ChildrenA}, {ancestor, ChildrenB}) ->
    {ancestor, ordsets:intersection(ChildrenA, ChildrenB)}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether Path is ancestor of PossibleSubPath. If it is then
%% returns additionally it's immediate child.
%% @end
%%--------------------------------------------------------------------
-spec is_ancestor(file_meta:path(), pos_integer(), file_meta:path()) ->
    {true, file_meta:name()} | false.
is_ancestor(Path, PathLen, PossibleSubPath) ->
    case PossibleSubPath of
        <<Path:PathLen/binary, "/", SubPath/binary>> ->
            [Name | _] = string:split(SubPath, <<"/">>),
            {true, Name};
        _ ->
            false
    end.


%% @private
-spec is_subpath(file_meta:path(), file_meta:path(), pos_integer()) ->
    boolean().
is_subpath(PossibleSubPath, Path, PathLen) ->
    case PossibleSubPath of
        <<Path:PathLen/binary, "/", _/binary>> ->
            true;
        _ ->
            false
    end.


%% @private
-spec is_path_or_subpath(file_meta:path(), file_meta:path()) -> boolean().
is_path_or_subpath(PossiblePathOrSubPath, Path) ->
    is_path_or_subpath(PossiblePathOrSubPath, Path, size(Path)).


%% @private
-spec is_path_or_subpath(file_meta:path(), file_meta:path(), pos_integer()) ->
    boolean().
is_path_or_subpath(Path, Path, _PathLen) ->
    true;
is_path_or_subpath(PossiblePathOrSubPath, Path, PathLen) ->
    is_subpath(PossiblePathOrSubPath, Path, PathLen).


%% @private
-spec objectids_to_guid_whitelist([file_id:objectid()]) -> guid_whitelist().
objectids_to_guid_whitelist(Objectids) ->
    lists:filtermap(fun(ObjectId) ->
        try
            {true, element(2, {ok, _} = file_id:objectid_to_guid(ObjectId))}
        catch _:_ ->
            % Invalid objectid does not make entire caveat/token invalid
            false
        end
    end, Objectids).


%% @private
-spec guids_to_paths([file_id:file_guid()]) -> [file_meta:path()].
guids_to_paths(Guids) ->
    lists:filtermap(fun(Guid) ->
        try
            {true, element(1, get_canonical_path(file_ctx:new_by_guid(Guid)))}
        catch _:_ ->
            % File may have been deleted so it is not possible to resolve
            % it's path
            false
        end
    end, Guids).


%% @private
-spec get_canonical_path(file_ctx:ctx()) -> {file_meta:path(), file_ctx:ctx()}.
get_canonical_path(FileCtx) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    {string:trim(Path, trailing, "/"), FileCtx2}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns file's guid stripped of share id.
%% @end
%%--------------------------------------------------------------------
-spec get_file_guid(file_ctx:ctx()) -> file_id:file_guid().
get_file_guid(FileCtx) ->
    file_id:share_guid_to_guid(file_ctx:get_guid_const(FileCtx)).


%% @private
-spec get_access_token(user_ctx:ctx()) -> tokens:serialized().
get_access_token(UserCtx) ->
    TokenCredentials = user_ctx:get_credentials(UserCtx),
    auth_manager:get_access_token(TokenCredentials).
