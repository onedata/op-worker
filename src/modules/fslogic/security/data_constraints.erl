%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(data_constraints).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_allow_all_constraints/0, get/1, verify/3]).

-ifdef(TEST).
-export([
    intersect_path_whitelists/2,
    consolidate_paths/1,

    is_subpath/2, is_path_or_subpath/2
]).
-endif.


-type path_whitelist() :: ordsets:ordset(file_meta:path()).
-type relation() :: subpath | {ancestor, gb_sets:set(file_meta:name())}.

-type allowed_paths() :: any | path_whitelist().
% List of guid lists. To verify if guid_constraints hold for some file
% one must check if every list contains either that file's guid or any
% of it's ancestors.
-type guid_constraints() :: any | [[file_id:file_guid()]].
-type constraints() :: {allowed_paths(), guid_constraints()}.

-export_type([constraints/0]).


-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_allow_all_constraints() -> constraints().
get_allow_all_constraints() -> {any, any}.


-spec get([caveats:caveat()]) ->
    {ok, constraints()} | {error, invalid_constraints}.
get(Caveats) ->
    #{paths := PathWhiteList, guids := GuidConstraints0} = lists:foldl(fun
        (?CV_PATH(Paths), #{paths := any} = Acc) ->
            Acc#{paths => consolidate_paths(Paths)};
        (?CV_PATH(_), #{paths := []} = Acc) ->
            Acc;
        (?CV_PATH(Paths), #{paths := CurrentIntersection} = Acc) ->
            Acc#{paths => intersect_path_whitelists(
                consolidate_paths(Paths), CurrentIntersection
            )};
        (?CV_OBJECTID(ObjectIds), #{guids := any} = Acc) ->
            Acc#{guids => [objectids_to_guids(ObjectIds)]};
        (?CV_OBJECTID(ObjectIds), #{guids := Guids} = Acc) ->
            Acc#{guids => [objectids_to_guids(ObjectIds) | Guids]};
        (_, Acc) ->
            Acc
    end, #{paths => any, guids => any}, Caveats),

    GuidConstraints1 = case GuidConstraints0 of
        any ->
            any;
        _ ->
            % Filter out failed translations from objectids to guids
            lists:filter(fun(GuidsSet) -> GuidsSet /= [] end, GuidConstraints0)
    end,

    case {PathWhiteList, GuidConstraints1} of
        {[], _} ->
            {error, invalid_constraints};
        {_, []} ->
            {error, invalid_constraints};
        DataConstraints ->
            {ok, DataConstraints}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies data constraints, that is whether access to specified file
%% can be granted. AllowAncestorsOfPaths means that access can be granted
%% not only for files/directories directly allowed by constraints but also
%% to their ancestors.
%% For some operations and in case of file being ancestor to paths allowed
%% by constraints it may be necessary to know list of file's immediate
%% children leading to paths allowed by constraints. That is why it is
%% also returned.
%% @end
%%--------------------------------------------------------------------
-spec verify(user_ctx:ctx(), file_ctx:ctx(), AllowAncestorsOfPaths :: boolean()) ->
    {ChildrenWhiteList :: undefined | [file_meta:name()], file_ctx:ctx()}.
verify(UserCtx, FileCtx, AllowAncestorsOfPaths) ->
    {AllowedPaths, GuidConstraints} = user_ctx:get_data_constraints(UserCtx),
    Result = check_and_cache_data_constraints(
        UserCtx, FileCtx,
        AllowedPaths, GuidConstraints, AllowAncestorsOfPaths
    ),
    case Result of
        {subpath, FileCtx} ->
            {undefined, FileCtx};
        {{ancestor, ChildrenWhiteList}, FileCtx} ->
            {ChildrenWhiteList, FileCtx}
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
-spec consolidate_paths([file_meta:path()]) -> path_whitelist().
consolidate_paths(Paths) ->
    TrimmedPaths = [string:trim(Path, trailing, "/") || Path <- Paths],
    consolidate_paths(ordsets:from_list(TrimmedPaths), []).


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
    allowed_paths(), guid_constraints(),
    AllowAncestorsOfPaths :: boolean()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_and_cache_data_constraints(_UserCtx, FileCtx, any, any, _) ->
    {subpath, FileCtx};
check_and_cache_data_constraints(
    UserCtx, FileCtx0, AllowedPaths, GuidConstraints, AllowAncestorsOfPaths
) ->
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    SessionDiscriminator = get_session_discriminator(UserCtx),
    CacheKey = {data_constraint, SessionDiscriminator, FileGuid},
    case permissions_cache:check_permission(CacheKey) of
        {ok, subpath} ->
            {subpath, FileCtx0};
        {ok, {ancestor, _ChildrenList} = Ancestor} ->
            case AllowAncestorsOfPaths of
                true -> {Ancestor, FileCtx0};
                false -> throw(?EACCES)
            end;
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            {ParentGuid, FileCtx1} = file_ctx:get_parent_guid(FileCtx0, UserCtx),
            ParentCacheKey = {data_constraint, SessionDiscriminator, ParentGuid},
            case permissions_cache:check_permission(ParentCacheKey) of
                {ok, subpath} ->
                    permissions_cache:cache_permission(CacheKey, subpath),
                    {subpath, FileCtx1};
                {ok, ?EACCES} ->
                    permissions_cache:cache_permission(CacheKey, ?EACCES),
                    throw(?EACCES);
                _ ->
                    % Situations when nothing is cached for parent or
                    % {ancestor, Children} is cached are not differentiated
                    % because knowledge that parent is ancestor does not
                    % tell whether file is also ancestor or subpath
                    try
                        {PathRel, FileCtx2} = check_data_path_constraints(
                            FileCtx1, AllowedPaths, AllowAncestorsOfPaths
                        ),
                        {GuidRel, FileCtx3} = check_data_guid_constraints(
                            UserCtx, SessionDiscriminator, FileCtx2,
                            GuidConstraints, AllowAncestorsOfPaths
                        ),
                        Relation = case intersect_relations(PathRel, GuidRel) of
                            subpath ->
                                subpath;
                            {ancestor, ChildrenSet} ->
                                {ancestor, gb_sets:to_list(ChildrenSet)}
                        end,
                        permissions_cache:cache_permission(CacheKey, Relation),
                        {Relation, FileCtx3}
                    catch throw:?EACCES ->
                        permissions_cache:cache_permission(CacheKey, ?EACCES),
                        throw(?EACCES)
                    end
            end
    end.


%% @private
-spec check_data_path_constraints(file_ctx:ctx(), allowed_paths(),
    AllowAncestorsOfPaths :: boolean()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_data_path_constraints(FileCtx, any, _AllowAncestorsOfPaths) ->
    {subpath, FileCtx};
check_data_path_constraints(FileCtx0, AllowedPaths, false) ->
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
check_data_path_constraints(FileCtx0, AllowedPaths, true) ->
    {FilePath, FileCtx1} = get_canonical_path(FileCtx0),

    case check_data_path_relation(FilePath, AllowedPaths) of
        undefined ->
            throw(?EACCES);
        subpath ->
            {subpath, FileCtx1};
        {ancestor, _Children} = Ancestor ->
            {Ancestor, FileCtx1}
    end.


%% @private
-spec check_data_guid_constraints(user_ctx:ctx(), SessionDiscriminator :: term(),
    file_ctx:ctx(), guid_constraints(), AllowAncestorsOfPaths :: boolean()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_data_guid_constraints(_, _, FileCtx, any, _AllowAncestorsOfPaths) ->
    {subpath, FileCtx};
check_data_guid_constraints(
    UserCtx, SessionDiscriminator, FileCtx0, GuidConstraints, false
) ->
    case does_fulfill_guid_constraints(
        UserCtx, SessionDiscriminator, FileCtx0, GuidConstraints
    ) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, _} ->
            throw(?EACCES)
    end;
check_data_guid_constraints(
    UserCtx, SessionDiscriminator, FileCtx0, GuidConstraints, true
) ->
    case does_fulfill_guid_constraints(
        UserCtx, SessionDiscriminator, FileCtx0, GuidConstraints
    ) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, FileCtx1} ->
            {FilePath, FileCtx2} = get_canonical_path(FileCtx1),

            Relation = lists:foldl(fun(GuidsList, CurrRelation) ->
                AllowedPaths = guids_to_paths(GuidsList),
                case check_data_path_relation(FilePath, AllowedPaths) of
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
-spec does_fulfill_guid_constraints(user_ctx:ctx(),
    SessionDiscriminator :: term(),
    file_ctx:ctx(), guid_constraints()
) ->
    {true, file_ctx:ctx()} |
    {false, guid_constraints(), file_ctx:ctx()}.
does_fulfill_guid_constraints(
    UserCtx, SessionDiscriminator, FileCtx, AllGuidConstraints
) ->
    FileGuid = get_file_guid(FileCtx),
    CacheKey = {guid_constraint, SessionDiscriminator, FileGuid},

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
                        UserCtx, SessionDiscriminator, ParentCtx,
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
%% Checks whether FilePath is ancestor or subpath to any of specified paths.
%% In case when FilePath is ancestor to one path and subpath to another, then
%% subpath takes precedence.
%% Additionally, if it is ancestor, it returns list of it's immediate
%% children.
%% @end
%%--------------------------------------------------------------------
-spec check_data_path_relation(file_meta:path(), [file_meta:path()]) ->
    undefined | relation().
check_data_path_relation(Path, AllowedPaths) ->
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
                    % Check if FilePath is ancestor to AllowedPath
                    case AllowedPath of
                        <<Path:PathLen/binary, "/", SubPath/binary>> ->
                            [Name | _] = string:split(SubPath, <<"/">>),
                            NamesAcc = case Acc of
                                undefined -> gb_sets:new();
                                {ancestor, Children} -> Children
                            end,
                            {ancestor, gb_sets:add(Name, NamesAcc)};
                        _ ->
                            Acc
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
    {ancestor, gb_sets:intersection(ChildrenA, ChildrenB)}.


%% @private
-spec is_subpath(file_meta:path(), file_meta:path()) -> boolean().
is_subpath(PossibleSubPath, Path) ->
    is_subpath(PossibleSubPath, Path, size(Path)).


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
is_path_or_subpath(PossiblePathOrSubPath, Path, PathLen) ->
    case PossiblePathOrSubPath of
        Path ->
            true;
        <<Path:PathLen/binary, "/", _/binary>> ->
            true;
        _ ->
            false
    end.


%% @private
-spec objectids_to_guids([file_id:objectid()]) -> [file_id:file_guid()].
objectids_to_guids(Objectids) ->
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
get_session_discriminator(UserCtx) ->
    case user_ctx:get_auth(UserCtx) of
        #token_auth{token = SerializedToken} ->
            SerializedToken;
        SessionAuth ->
            SessionAuth
    end.
