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
-export([get_allow_all_constraints/0, get/1]).

-ifdef(TEST).
-export([
    intersect_path_whitelists/2,
    consolidate_paths/1,

    is_subpath/2, is_path_or_subpath/2
]).
-endif.


-type path_whitelist() :: ordsets:ordset(file_meta:path()).

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
