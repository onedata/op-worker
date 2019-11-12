%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for authorization of fslogic operations.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_authz).
-author("Bartosz Walkowicz").

-include("proto/common/credentials.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    ensure_authorized/3, ensure_authorized/4,
    ensure_authorized_readdir/3
]).

-ifdef(TEST).
-export([check_data_path_relation/2, is_subpath/2]).
-endif.

-type children_list() :: [file_meta:name()].
-type children_set() :: gb_sets:set(file_meta:name()).
-type relation() :: subpath | {ancestor, children_set()}.

-type relation_ctx() ::
    {subpath, file_ctx:ctx()} |
    {{ancestor, children_list()}, file_ctx:ctx()}.

-export_type([relation_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv authorize(UserCtx, FileCtx0, AccessRequirements, true).
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()]) -> file_ctx:ctx() | no_return().
ensure_authorized(UserCtx, FileCtx0, AccessRequirements) ->
    ensure_authorized(UserCtx, FileCtx0, AccessRequirements, false).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data caveats.
%% AllowAncestorsOfPaths means that permission can be granted not only
%% for files in subpaths allowed by caveats but also for their ancestors.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()], boolean()) -> file_ctx:ctx().
ensure_authorized(
    UserCtx, FileCtx0, AccessRequirements, AllowAncestorsOfPaths
) ->
    {_, FileCtx1} = check_and_cache_data_constraints(
        UserCtx, FileCtx0, AllowAncestorsOfPaths
    ),
    fslogic_access:assert_granted(UserCtx, FileCtx1, AccessRequirements).


%%--------------------------------------------------------------------
%% @doc
%% Checks access to specified file and verifies data caveats.
%% If check concerns file which is ancestor to any allowed by caveats then
%% list of allowed (by caveats) children is also returned.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized_readdir(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()]) -> relation_ctx().
ensure_authorized_readdir(UserCtx, FileCtx0, AccessRequirements) ->
    FileCtx1 = fslogic_access:assert_granted(
        UserCtx, FileCtx0, AccessRequirements
    ),
    check_and_cache_data_constraints(UserCtx, FileCtx1, true).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_and_cache_data_constraints(user_ctx:ctx(), file_ctx:ctx(),
    boolean()) -> relation_ctx().
check_and_cache_data_constraints(UserCtx, FileCtx, AllowAncestorsOfPaths) ->
    {AllowedPaths, GuidConstraints} = user_ctx:get_data_constraints(UserCtx),
    check_and_cache_data_constraints(
        UserCtx, FileCtx, AllowedPaths, GuidConstraints, AllowAncestorsOfPaths
    ).


%% @private
-spec check_and_cache_data_constraints(user_ctx:ctx(), file_ctx:ctx(),
    token_utils:allowed_paths(), token_utils:guid_constraints(),
    AllowAncestorsOfPaths :: boolean()
) ->
    relation_ctx() | no_return().
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
                        Relation = case converge_relations(PathRel, GuidRel) of
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
-spec check_data_path_constraints(file_ctx:ctx(), token_utils:allowed_paths(),
    boolean()) -> {relation(), file_ctx:ctx()} | no_return().
check_data_path_constraints(FileCtx, any, _AllowAncestorsOfPaths) ->
    {subpath, FileCtx};
check_data_path_constraints(FileCtx0, AllowedPaths, false) ->
    {FilePath0, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    FilePath1 = string:trim(FilePath0, trailing, "/"),

    IsFileInAllowedSubPath = lists:any(fun(AllowedPath) ->
        is_subpath(FilePath1, AllowedPath)
    end, AllowedPaths),

    case IsFileInAllowedSubPath of
        true ->
            {subpath, FileCtx1};
        false ->
            throw(?EACCES)
    end;
check_data_path_constraints(FileCtx0, AllowedPaths, true) ->
    {FilePath0, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
    FilePath1 = string:trim(FilePath0, trailing, "/"),

    case check_data_path_relation(FilePath1, AllowedPaths) of
        undefined ->
            throw(?EACCES);
        subpath ->
            {subpath, FileCtx1};
        {ancestor, _Children} = Ancestor ->
            {Ancestor, FileCtx1}
    end.


%% @private
-spec check_data_guid_constraints(user_ctx:ctx(), SessionDiscriminator :: term(),
    file_ctx:ctx(), token_utils:guid_constraints(), boolean()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_data_guid_constraints(_, _, FileCtx, any, _AllowAncestorsOfPaths) ->
    {subpath, FileCtx};
check_data_guid_constraints(
    UserCtx, SessionDiscriminator, FileCtx0, AllowedGuids, false
) ->
    case is_guid_allowed(UserCtx, SessionDiscriminator, FileCtx0, AllowedGuids) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, _} ->
            throw(?EACCES)
    end;
check_data_guid_constraints(
    UserCtx, SessionDiscriminator, FileCtx0, AllowedGuids, true
) ->
    case is_guid_allowed(UserCtx, SessionDiscriminator, FileCtx0, AllowedGuids) of
        {true, FileCtx1} ->
            {subpath, FileCtx1};
        {false, _, FileCtx1} ->
            {FilePath0, FileCtx2} = file_ctx:get_canonical_path(FileCtx1),
            FilePath1 = string:trim(FilePath0, trailing, "/"),

            lists:foldl(fun(GuidsSet, {CurrRelation, CurrFileCtx}) ->
                AllowedPaths = guids_to_paths(GuidsSet),
                case check_data_path_relation(FilePath1, AllowedPaths) of
                    undefined ->
                        throw(?EACCES);
                    Relation ->
                        {converge_relations(CurrRelation, Relation), CurrFileCtx}
                end
            end, {subpath, FileCtx2}, AllowedGuids)
    end.


-spec is_guid_allowed(user_ctx:ctx(), SessionDiscriminator :: term(),
    file_ctx:ctx(), token_utils:guid_constraints()
) ->
    {true, file_ctx:ctx()} |
    {false, token_utils:guid_constraints(), file_ctx:ctx()}.
is_guid_allowed(UserCtx, SessionDiscriminator, FileCtx, AllGuidSets) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    CacheKey = {guid_constraint, SessionDiscriminator, FileGuid},

    case permissions_cache:check_permission(CacheKey) of
        {ok, true} ->
            {true, FileCtx};
        {ok, {false, NotFulfilledGuidSets}} ->
            {false, NotFulfilledGuidSets, FileCtx};
        _ ->
            case file_ctx:is_root_dir_const(FileCtx) of
                true ->
                    RemainingGuidSets = lists:filter(fun(GuidsSet) ->
                        not lists:member(FileGuid, GuidsSet)
                    end, AllGuidSets),
                    permissions_cache:cache_permission(
                        CacheKey, {false, RemainingGuidSets}
                    ),
                    {false, RemainingGuidSets, FileCtx};
                false ->
                    {ParentCtx, FileCtx1} = file_ctx:get_parent(FileCtx, UserCtx),
                    case is_guid_allowed(UserCtx, SessionDiscriminator, ParentCtx, AllGuidSets) of
                        {true, _} ->
                            permissions_cache:cache_permission(CacheKey, true),
                            {true, FileCtx1};
                        {false, RemainingGuidSets0, _} ->
                            RemainingGuidSets1 = lists:filter(fun(GuidsSet) ->
                                not lists:member(FileGuid, GuidsSet)
                            end, RemainingGuidSets0),
                            case RemainingGuidSets1 of
                                [] ->
                                    permissions_cache:cache_permission(CacheKey, true),
                                    {true, FileCtx1};
                                _ ->
                                    permissions_cache:cache_permission(
                                        CacheKey, {false, RemainingGuidSets1}
                                    ),
                                    {false, RemainingGuidSets1, FileCtx1}
                            end
                    end
            end
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
                    % Check if FilePath is allowed by caveats
                    % (AllowedPath is ancestor to FilePath or FilePath itself)
                    case is_subpath(Path, AllowedPath, AllowedPathLen) of
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
-spec is_subpath(file_meta:path(), file_meta:path()) -> boolean().
is_subpath(PossibleSubPath, Path) ->
    is_subpath(PossibleSubPath, Path, size(Path)).


%% @private
-spec is_subpath(file_meta:path(), file_meta:path(), pos_integer()) ->
    boolean().
is_subpath(PossibleSubPath, Path, PathLen) ->
    case PossibleSubPath of
        Path ->
            true;
        <<Path:PathLen/binary, "/", _/binary>> ->
            true;
        _ ->
            false
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns intersection of relation sets.
%% @end
%%--------------------------------------------------------------------
-spec converge_relations(relation(), relation()) -> relation().
converge_relations(subpath, subpath) ->
    subpath;
converge_relations({ancestor, _Children} = Ancestor, subpath) ->
    Ancestor;
converge_relations(subpath, {ancestor, _Children} = Ancestor) ->
    Ancestor;
converge_relations({ancestor, ChildrenA}, {ancestor, ChildrenB}) ->
    {ancestor, gb_sets:intersection(ChildrenA, ChildrenB)}.


%% @private
-spec guids_to_paths([file_id:file_guid()]) -> [file_meta:path()].
guids_to_paths(Guids) ->
    lists:filtermap(fun(Guid) ->
        try
            FileCtx = file_ctx:new_by_guid(Guid),
            {Path, _} = file_ctx:get_canonical_path(FileCtx),
            {true, string:trim(Path, trailing, "/")}
        catch _:_ ->
            % File may have been deleted so it is not possible to resolve
            % it's path
            false
        end
    end, Guids).


%% @private
get_session_discriminator(UserCtx) ->
    case user_ctx:get_auth(UserCtx) of
        #token_auth{token = SerializedToken} ->
            SerializedToken;
        SessionAuth ->
            SessionAuth
    end.
