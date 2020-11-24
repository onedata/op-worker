%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles operations on data constraints (e.g. verifying
%%% whether access to file should be allowed).
%%% There are 3 types of constraints:
%%% - allowed_paths    - list of paths which are allowed (also their descendants).
%%%                      To verify if allowed_paths hold for some file one must
%%%                      check if file path is contained in list or is descendant
%%%                      of any path from list,
%%% - guid_constraints - list of guid whitelists. To verify if guid_constraints
%%%                      hold for some file one must check if every whitelist
%%%                      contains either that file's guid or any
%%%                      of its ancestors,
%%% - readonly mode    - flag telling whether only operations available in
%%%                      readonly mode can be performed.
%%%
%%%                              !!! NOTE !!!
%%% Sometimes access may be granted not only to paths or descendants allowed
%%% by constraints directly but also to those paths ancestors (operations like
%%% readdir, stat, resolve_path/guid, get_parent). For such cases whitelist
%%% containing file's immediate children leading to paths allowed by
%%% constraints is also returned (may be useful to e.g. filter
%%% readdir result).
%%%
%%% To see examples of how function in this module works please check
%%% data_constraints_test.erl
%%% @end
%%%-------------------------------------------------------------------
-module(data_constraints).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/onedata.hrl").


%% API
-export([get_allow_all_constraints/0, get/1]).
-export([assert_not_readonly_mode/1, inspect/4]).


% Specific file relation to files specified in constraints set by token caveats
-type constraint_relation() :: {ancestor, ordsets:ordset(file_meta:name())} | equal_or_descendant.

-type allowed_paths() :: any | [file_meta:path()].

-type guid_whitelist() :: [file_id:file_guid()].
-type guid_constraints() :: any | [guid_whitelist()].

-record(constraints, {
    paths :: allowed_paths(),
    guids :: guid_constraints(),
    readonly = false :: boolean()
}).
-opaque constraints() :: #constraints{}.

-type ancestor_policy() :: allow_ancestors | disallow_ancestors.

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
            Acc#constraints{paths = sanitize_and_consolidate_paths(Paths)};
        (?CV_PATH(_Paths), #constraints{paths = []} = Acc) ->
            Acc;
        (?CV_PATH(Paths), #constraints{paths = AllowedPaths} = Acc) ->
            Acc#constraints{paths = filepath_utils:intersect(
                sanitize_and_consolidate_paths(Paths), AllowedPaths
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
%%
%%                            !!! NOTE !!!
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
                {equal_or_descendant, FileCtx1} ->
                    {undefined, FileCtx1};
                {{ancestor, ChildrenWhiteList}, FileCtx1} ->
                    {ChildrenWhiteList, FileCtx1}
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec sanitize_and_consolidate_paths([file_meta:path()]) -> [file_meta:path()].
sanitize_and_consolidate_paths(RawPaths) ->
    Paths = lists:filtermap(fun(Path) ->
        case filepath_utils:sanitize(Path) of
            {ok, SanitizedPath} -> {true, SanitizedPath};
            {error, _} -> false
        end
    end, RawPaths),

    filepath_utils:consolidate(Paths).


%% @private
-spec check_and_cache_data_constraints(user_ctx:ctx(), file_ctx:ctx(),
    constraints(), ancestor_policy()
) ->
    {constraint_relation(), file_ctx:ctx()} | no_return().
check_and_cache_data_constraints(UserCtx, FileCtx0, #constraints{
    paths = AllowedPaths,
    guids = GuidConstraints
}, AncestorPolicy) ->
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    SerializedToken = get_access_token(UserCtx),
    CacheKey = {data_constraint, SerializedToken, FileGuid},
    case permissions_cache:check_permission(CacheKey) of
        {ok, equal_or_descendant} ->
            % File is allowed by constraints - every operations are permitted
            {equal_or_descendant, FileCtx0};
        {ok, {equal_or_descendant, ?EACCES}} when AncestorPolicy =:= disallow_ancestors ->
            % File is not allowed by constraints but it may be ancestor to some
            % of allowed paths (such checks were not performed)
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
                Result = intersect_constraint_relations(PathRel, GuidRel),
                permissions_cache:cache_permission(CacheKey, Result),
                {Result, FileCtx2}
            catch throw:?EACCES ->
                case AncestorPolicy of
                    allow_ancestors ->
                        permissions_cache:cache_permission(CacheKey, ?EACCES);
                    disallow_ancestors ->
                        permissions_cache:cache_permission(CacheKey, {equal_or_descendant, ?EACCES})
                end,
                throw(?EACCES)
            end
    end.


%% @private
-spec check_allowed_paths(file_ctx:ctx(), allowed_paths(), ancestor_policy()) ->
    {constraint_relation(), file_ctx:ctx()} | no_return().
check_allowed_paths(FileCtx, any, _AncestorPolicy) ->
    % 'any' allows all descendants of "/" - effectively all files
    {equal_or_descendant, FileCtx};
check_allowed_paths(FileCtx0, AllowedPaths, disallow_ancestors) ->
    {FilePath, FileCtx1} = get_canonical_path(FileCtx0),

    IsAllowed = lists:any(fun(AllowedPath) ->
        case filepath_utils:is_equal_or_descendant(FilePath, AllowedPath) of
            {true, _} -> true;
            false -> false
        end
    end, AllowedPaths),

    case IsAllowed of
        true ->
            {equal_or_descendant, FileCtx1};
        false ->
            throw(?EACCES)
    end;
check_allowed_paths(FileCtx0, AllowedPaths, allow_ancestors) ->
    {FilePath, FileCtx1} = get_canonical_path(FileCtx0),

    case check_path_constraint_relation(FilePath, AllowedPaths) of
        undefined ->
            throw(?EACCES);
        PathRelation ->
            {PathRelation, FileCtx1}
    end.


%% @private
-spec check_guid_constraints(user_ctx:ctx(), tokens:serialized(),
    file_ctx:ctx(), guid_constraints(), ancestor_policy()
) ->
    {constraint_relation(), file_ctx:ctx()} | no_return().
check_guid_constraints(_, _, FileCtx, any, _AncestorPolicy) ->
    % 'any' allows all descendants of UserRootDir ("/") - effectively all files
    {equal_or_descendant, FileCtx};
check_guid_constraints(
    UserCtx, SerializedToken, FileCtx0, GuidConstraints, disallow_ancestors
) ->
    case does_fulfill_guid_constraints(
        UserCtx, SerializedToken, FileCtx0, GuidConstraints
    ) of
        {true, FileCtx1} ->
            {equal_or_descendant, FileCtx1};
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
            {equal_or_descendant, FileCtx1};
        {false, _, FileCtx1} ->
            {FilePath, FileCtx2} = get_canonical_path(FileCtx1),

            Relation = lists:foldl(fun(GuidsList, CurrRelation) ->
                AllowedPaths = guids_to_canonical_paths(GuidsList),
                case check_path_constraint_relation(FilePath, AllowedPaths) of
                    undefined ->
                        throw(?EACCES);
                    PathRelation ->
                        intersect_constraint_relations(CurrRelation, PathRelation)
                end
            end, equal_or_descendant, GuidConstraints),

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
    FileGuid = get_file_bare_guid(FileCtx),
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
    FileGuid = get_file_bare_guid(FileCtx),
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


%% @private
-spec check_path_constraint_relation(
    Path :: filepath_utils:sanitized_path(),
    AllowedPaths :: [filepath_utils:sanitized_path()]
) ->
    undefined | constraint_relation().
check_path_constraint_relation(Path, ReferencePaths) ->
    lists_utils:foldl_while(fun(ReferencePath, Acc) ->
        case filepath_utils:check_relation(Path, ReferencePath) of
            undefined ->
                {cont, Acc};
            {ancestor, RelPathToDescendant} ->
                [Name | _] = string:split(RelPathToDescendant, <<?DIRECTORY_SEPARATOR>>),
                NamesAcc = case Acc of
                    undefined -> ordsets:new();
                    {ancestor, Children} -> Children
                end,
                {cont, {ancestor, ordsets:add_element(Name, NamesAcc)}};
            equal ->
                {halt, equal_or_descendant};
            {descendant, _} ->
                {halt, equal_or_descendant}
        end
    end, undefined, ReferencePaths).


%% @private
-spec intersect_constraint_relations(constraint_relation(), constraint_relation()) ->
    constraint_relation().
intersect_constraint_relations(equal_or_descendant, equal_or_descendant) ->
    equal_or_descendant;
intersect_constraint_relations({ancestor, _Children} = Ancestor, equal_or_descendant) ->
    Ancestor;
intersect_constraint_relations(equal_or_descendant, {ancestor, _Children} = Ancestor) ->
    Ancestor;
intersect_constraint_relations({ancestor, ChildrenA}, {ancestor, ChildrenB}) ->
    {ancestor, ordsets:intersection(ChildrenA, ChildrenB)}.


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
-spec guids_to_canonical_paths([file_id:file_guid()]) -> [file_meta:path()].
guids_to_canonical_paths(Guids) ->
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
-spec get_file_bare_guid(file_ctx:ctx()) -> file_id:file_guid().
get_file_bare_guid(FileCtx) ->
    file_id:share_guid_to_guid(file_ctx:get_guid_const(FileCtx)).


%% @private
-spec get_access_token(user_ctx:ctx()) -> tokens:serialized().
get_access_token(UserCtx) ->
    TokenCredentials = user_ctx:get_credentials(UserCtx),
    auth_manager:get_access_token(TokenCredentials).
