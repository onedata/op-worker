%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules provides utility functions for token management in op.
%%% @end
%%%-------------------------------------------------------------------
-module(token_utils).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    assert_interface_allowed/2,
    assert_no_data_caveats/1,
    verify_api_caveats/3,

    get_data_constraints/1,
    sanitize_cv_data_path/1
]).

-ifdef(TEST).
-export([
    converge_paths/2,
    consolidate_paths/1
]).
-endif.

-type interface() :: gui | rest | oneclient.

-define(DATA_CAVEATS, [cv_data_path, cv_data_objectid]).

-define(CV_PATH(__PATHS), #cv_data_path{whitelist = __PATHS}).
-define(CV_OBJECTID(__OBJECTIDS), #cv_data_objectid{whitelist = __OBJECTIDS}).


%%%===================================================================
%%% API
%%%===================================================================


-spec assert_interface_allowed([caveats:caveat()] | tokens:serialized(),
    interface()) -> ok | no_return().
assert_interface_allowed(Caveats, _Interface) when is_list(Caveats) ->
    % TODO VFS-5719 check interface caveats
    ok;
assert_interface_allowed(SerializedToken, Interface) ->
    case tokens:deserialize(SerializedToken) of
        {ok, Token} ->
            Caveats = tokens:get_caveats(Token),
            assert_interface_allowed(Caveats, Interface);
        {error, _} = Error ->
            throw(Error)
    end.


-spec assert_no_data_caveats([caveats:caveat()]) -> ok | no_return().
assert_no_data_caveats(Caveats) when is_list(Caveats) ->
    case caveats:filter(?DATA_CAVEATS, Caveats) of
        [] ->
            ok;
        [DataCaveat | _] ->
            throw(?ERROR_TOKEN_CAVEAT_UNVERIFIED(DataCaveat))
    end.


-spec verify_api_caveats([caveats:caveat()], op_logic:operation(), gri:gri()) ->
    ok | no_return().
verify_api_caveats(Caveats, Operation, GRI) ->
    lists:foreach(fun(ApiCaveat) ->
        case cv_api:verify(ApiCaveat, ?OP_WORKER, Operation, GRI) of
            true -> ok;
            false -> throw(?ERROR_TOKEN_CAVEAT_UNVERIFIED(ApiCaveat))
        end
    end, Caveats).


-spec get_data_constraints([caveats:caveat()]) ->
    {
        AllowedPaths :: all | [file_meta:path()],
        AllowedGuidSets :: all | [[file_id:file_guid()]]
    }.
get_data_constraints(Caveats) ->
    #{paths := Paths, guids := Guids} = lists:foldl(fun
        (#cv_data_path{} = PathCaveat, #{paths := all} = Acc) ->
            ?CV_PATH(AllowedPaths) = sanitize_cv_data_path(PathCaveat),
            Acc#{paths => AllowedPaths};
        (#cv_data_path{}, #{paths := []} = Acc) ->
            Acc;
        (#cv_data_path{} = PathCaveat, #{paths := Intersection} = Acc) ->
            ?CV_PATH(AllowedPaths) = sanitize_cv_data_path(PathCaveat),
            Acc#{paths => converge_paths(AllowedPaths, Intersection)};
        (?CV_OBJECTID(ObjectIds), #{guids := Guids} = Acc) ->
            Acc#{guids => [objectids_to_guids(ObjectIds) | Guids]};
        (_, Acc) ->
            Acc
    end, #{paths => all, guids => []}, Caveats),

    case Guids of
        [] -> {Paths, all};
        _ -> {Paths, Guids}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sanitizes paths by removing their trailing slashes and consolidates
%% them by removing ones that are subpaths of others (e.g consolidation
%% of [/a/b/, /a/b/c, /q/w/e] results in [/a/b, /q/w/e]).
%% @end
%%--------------------------------------------------------------------
sanitize_cv_data_path(#cv_data_path{whitelist = Paths}) ->
    TrimmedPaths = [string:trim(Path, trailing, "/") || Path <- Paths],
    #cv_data_path{whitelist = consolidate_paths(TrimmedPaths)}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns intersection of 2 sets of paths.
%% @end
%%--------------------------------------------------------------------
-spec converge_paths([file_meta:path()], [file_meta:path()]) ->
    [file_meta:path()].
converge_paths(PathsA, PathsB) ->
    lists:reverse(converge_paths(PathsA, PathsB, [])).


%% @private
-spec converge_paths([file_meta:path()], [file_meta:path()],
    [file_meta:path()]) -> [file_meta:path()].
converge_paths([], _, Intersection) ->
    Intersection;
converge_paths(_, [], Intersection) ->
    Intersection;
converge_paths([PathA | RestA] = A, [PathB | RestB] = B, Intersection) ->
    PathALen = size(PathA),
    PathBLen = size(PathB),

    case PathA < PathB of
        true ->
            case PathB of
                <<PathA:PathALen/binary, "/", _/binary>> ->
                    converge_paths(RestA, RestB, [PathB | Intersection]);
                _ ->
                    converge_paths(RestA, B, Intersection)
            end;
        false ->
            case PathA of
                PathB ->
                    converge_paths(RestA, RestB, [PathA | Intersection]);
                <<PathB:PathBLen/binary, "/", _/binary>> ->
                    converge_paths(RestA, RestB, [PathA | Intersection]);
                _ ->
                    converge_paths(A, RestB, Intersection)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Consolidates paths by removing ones that are subpaths of others (e.g
%% consolidation of [/a/b/, /a/b/c, /q/w/e] results in [/a/b, /q/w/e]).
%% @end
%%--------------------------------------------------------------------
-spec consolidate_paths([file_meta:path()]) -> [file_meta:path()].
consolidate_paths(Paths) ->
    lists:reverse(consolidate_paths(lists:usort(Paths), [])).


%% @private
-spec consolidate_paths([file_meta:path()], [file_meta:path()]) ->
    [file_meta:path()].
consolidate_paths([], ConsolidatedPaths) ->
    ConsolidatedPaths;
consolidate_paths([Path], ConsolidatedPaths) ->
    [Path | ConsolidatedPaths];
consolidate_paths([PathA, PathB | RestOfPaths], ConsolidatedPaths) ->
    PathALen = size(PathA),
    case PathB of
        PathA ->
            consolidate_paths([PathA | RestOfPaths], ConsolidatedPaths);
        <<PathA:PathALen/binary, "/", _/binary>> ->
            consolidate_paths([PathA | RestOfPaths], ConsolidatedPaths);
        _ ->
            consolidate_paths([PathB | RestOfPaths], [PathA | ConsolidatedPaths])
    end.
