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
    verify_api_caveats/3
]).

-ifdef(TEST).
-export([
    converge_paths/2,
    consolidate_paths/1
]).
-endif.

-type interface() :: gui | rest | oneclient.

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


-spec verify_api_caveats([caveats:caveat()], op_logic:operation(), gri:gri()) ->
    ok | no_return().
verify_api_caveats(Caveats, Operation, GRI) ->
    lists:foreach(fun(ApiCaveat) ->
        case cv_api:verify(ApiCaveat, ?OP_WORKER, Operation, GRI) of
            true -> ok;
            false -> throw(?ERROR_TOKEN_CAVEAT_UNVERIFIED(ApiCaveat))
        end
    end, Caveats).


-spec get_allowed_paths([#cv_data_path{}]) -> all | [file_meta:path()].
get_allowed_paths([]) ->
    all;
get_allowed_paths([?CV_PATH(Paths) | Rest]) ->
    lists:foldl(fun(?CV_PATH(AllowedPaths), Intersection) ->
        converge_paths(
            consolidate_paths(AllowedPaths),
            Intersection
        )
    end, consolidate_paths(Paths), Rest).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
%% Sanitizes paths by removing their trailing slashes and consolidates
%% them by removing ones that are subpaths of others (e.g consolidation
%% of [/a/b/, /a/b/c, /q/w/e] results in [/a/b, /q/w/e]).
%% @end
%%--------------------------------------------------------------------
consolidate_paths(Paths) ->
    SanitizedPaths = [string:trim(Path, trailing, "/") || Path <- Paths],
    lists:reverse(consolidate_paths(lists:usort(SanitizedPaths), [])).


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
