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

-type allowed_paths() :: all | [file_meta:path()].
-type allowed_guids() :: all | [[file_id:file_guid()]].


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
%% AllowAncestorsOfLocationCaveats means that permission can be granted
%% not only for files in subpaths allowed by caveats but also for their
%% ancestors.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(user_ctx:ctx(), file_ctx:ctx(),
    [fslogic_access:requirement()], boolean()) -> file_ctx:ctx().
ensure_authorized(
    UserCtx, FileCtx0, AccessRequirements, AllowAncestorsOfPaths
) ->
    {_, FileCtx1} = check_data_and_cache_constraints(
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
    [fslogic_access:requirement()]
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), children_list()}.
ensure_authorized_readdir(UserCtx, FileCtx0, AccessRequirements) ->
    FileCtx1 = fslogic_access:assert_granted(
        UserCtx, FileCtx0, AccessRequirements
    ),
    check_data_and_cache_constraints(UserCtx, FileCtx1, true).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_data_and_cache_constraints(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), children_list()} |
    no_return().
check_data_and_cache_constraints(UserCtx, FileCtx, AllowAncestorsOfPaths) ->
    {AllowedPaths, AllowedGuids} = user_ctx:get_data_constraints(UserCtx),
    check_and_cache_data_constraints(
        UserCtx, FileCtx, AllowedPaths, AllowedGuids, AllowAncestorsOfPaths
    ).


%% @private
-spec check_and_cache_data_constraints(user_ctx:ctx(), file_ctx:ctx(), allowed_paths(),
    allowed_guids(), AllowAncestorsOfPaths :: boolean()
) ->
    {subpath, file_ctx:ctx()} |
    {ancestor, file_ctx:ctx(), children_list()} |
    no_return().
check_and_cache_data_constraints(_UserCtx, FileCtx, all, all, _) ->
    {subpath, FileCtx};
check_and_cache_data_constraints(
    UserCtx, FileCtx0, AllowedPaths, AllowedGuids, AllowAncestorsOfPaths
) ->
    FileGuid = file_ctx:get_guid_const(FileCtx0),
    SessionDiscriminator = case user_ctx:get_auth(UserCtx) of
        #token_auth{token = SerializedToken} ->
            SerializedToken;
        SessionAuth ->
            SessionAuth
    end,
    CacheKey = {data_constraint, SessionDiscriminator, FileGuid},
    case permissions_cache:check_permission(CacheKey) of
        {ok, subpath} ->
            {subpath, FileCtx0};
        {ok, {ancestor, Children}} ->
            case AllowAncestorsOfPaths of
                true -> {ancestor, FileCtx0, Children};
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
                        Relation = check_data_constraints(
                            UserCtx, FileCtx1, AllowedPaths, AllowedGuids,
                            AllowAncestorsOfPaths
                        ),
                        sanitize_and_cache_relation(CacheKey, Relation)
                    catch throw:?EACCES ->
                        permissions_cache:cache_permission(CacheKey, ?EACCES),
                        throw(?EACCES)
                    end
            end
    end.


%% @private
-spec sanitize_and_cache_relation(CacheKey :: term(), relation()) ->
    {subpath, file_ctx:ctx()} | {ancestor, file_ctx:ctx(), children_list()}.
sanitize_and_cache_relation(CacheKey, {subpath, _FileCtx} = Rel) ->
    permissions_cache:cache_permission(CacheKey, subpath),
    Rel;
sanitize_and_cache_relation(CacheKey, {{ancestor, ChildrenSet}, FileCtx}) ->
    ChildrenList = gb_sets:to_list(ChildrenSet),
    CacheVal = {ancestor, ChildrenList},
    permissions_cache:cache_permission(CacheKey, CacheVal),
    {ancestor, FileCtx, ChildrenList}.


%% @private
-spec check_data_constraints(user_ctx:ctx(), file_ctx:ctx(), allowed_paths(),
    allowed_guids(), AllowAncestorsOfPaths :: boolean()
) ->
    {relation(), file_ctx:ctx()} | no_return().
check_data_constraints(_, FileCtx0, AllowedPaths, AllowedGuids, AllowAncestorsOfPaths) ->
    {PathRel, FileCtx1} = check_data_path_constraints(
        FileCtx0, AllowedPaths, AllowAncestorsOfPaths
    ),
    {GuidRel, FileCtx2} = check_data_guid_constraint(
        FileCtx1, AllowedGuids, AllowAncestorsOfPaths
    ),
    case {PathRel, GuidRel} of
        {subpath, subpath} ->
            {subpath, FileCtx2};
        {{ancestor, _Children} = Ancestor, subpath} ->
            {Ancestor, FileCtx2};
        {subpath, {ancestor, _Children} = Ancestor} ->
            {Ancestor, FileCtx2};
        {{ancestor, ChildrenA}, {ancestor, ChildrenB}} ->
            {{ancestor, gb_sets:intersection(ChildrenA, ChildrenB)}, FileCtx2}
    end.


%% @private
-spec check_data_guid_constraint(file_ctx:ctx(), all | [file_meta:path()],
    boolean()) -> {relation(), file_ctx:ctx()} | no_return().
check_data_guid_constraint(FileCtx, _, _AllowAncestorsOfPaths) ->
    {subpath, FileCtx}.
%%check_data_guid_constraint(FileCtx0, AllowedGuids, false) ->
%%    case is_guid_allowed(UserCtx, SessionDiscriminator, FileCtx0, AllowedGuids) of
%%        {true, FileCtx1} ->
%%            {subpath, FileCtx1};
%%        {false, _} ->
%%            throw(?EACCES)
%%    end;
%%check_data_guid_constraint(FileCtx0, AllowedGuids, true) ->
%%    case is_guid_allowed() of
%%        true ->
%%            {subpath, FileCtx0};
%%        false ->
%%            {FilePath0, FileCtx1} = file_ctx:get_canonical_path(FileCtx0),
%%            FilePath1 = string:trim(FilePath0, trailing, "/"),
%%            AllowedPaths = guids_to_paths(AllowedGuids),
%%            case check_data_path_relation(FilePath1, AllowedPaths) of
%%                undefined ->
%%                    throw(?EACCES);
%%                subpath ->
%%                    {subpath, FileCtx1};
%%                {ancestor, Children} ->
%%                    {ancestor, FileCtx1, Children}
%%            end
%%    end.
%%
%%
%%is_guid_allowed(UserCtx, SessionDiscriminator, FileCtx, AllowedGuids0) ->
%%    Guid = file_ctx:get_guid_const(FileCtx),
%%    CacheKey = {guid_constraint, SessionDiscriminator, Guid},
%%
%%    case permissions_cache:check_permission(CacheKey) of
%%        {ok, true} ->
%%            {true, FileCtx};
%%        {ok, false} ->
%%            {false, FileCtx};
%%        _ ->
%%            AllowedGuids1 = lists:filter(fun(GuidsSet) ->
%%                not lists:member(Guid, GuidsSet)
%%            end, AllowedGuids0),
%%            case AllowedGuids1 of
%%                [] ->
%%                    {true, FileCtx};
%%                _ ->
%%                    {FileCtx1, ParentCtx} = file_ctx:get_parent(FileCtx, UserCtx),
%%                    {IsParentAllowed, _} = is_guid_allowed(
%%                        UserCtx, SessionDiscriminator,
%%                        ParentCtx, AllowedGuids1
%%                    ),
%%                    permissions_cache:cache_permission(CacheKey, IsParentAllowed),
%%                    {IsParentAllowed, FileCtx1}
%%            end
%%    end.


%% @private
-spec check_data_path_constraints(file_ctx:ctx(), all | [file_meta:path()],
    boolean()) -> {relation(), file_ctx:ctx()} | no_return().
check_data_path_constraints(FileCtx, all, _AllowAncestorsOfPaths) ->
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
