%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(paths_cache).
-author("Jakub Kudzia").
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init_caches/1, invalidate_paths_caches/1]).
-export([get_canonical_paths_cache_name/1, get_uuid_based_paths_cache_name/1]).

-define(PATH_CACHE_GROUP, <<"paths_cache_group">>).
-define(CANONICAL_PATHS_CACHE_NAME(SpaceId), binary_to_atom(<<"canonical_paths_cache_", SpaceId/binary>>, utf8)).
-define(UUID_BASED_PATHS_CACHE_NAME(SpaceId), binary_to_atom(<<"uuid_paths_cache_", SpaceId/binary>>, utf8)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_group() -> ok.
init_group() ->
    CheckFrequency = application:get_env(?APP_NAME, canonical_paths_cache_frequency, 30000),
    Size = application:get_env(?APP_NAME, canonical_paths_cache_size, 20000),
    ok = bounded_cache:init_group(?PATH_CACHE_GROUP, #{
        check_frequency => CheckFrequency,
        size => Size,
        worker => true
    }).


-spec init_caches(od_space:id() | all) -> ok.
init_caches(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init_caches/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize paths bounded caches due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize paths bounded caches due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize paths bounded caches due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize paths bounded caches due to: ~p", [{Error2, Reason}])
    end;
init_caches(Space) ->
    ok = init(Space, get_canonical_paths_cache_name(Space)),
    ok = init(Space, get_uuid_based_paths_cache_name(Space)).


%%-------------------------------------------------------------------
%% @doc
%% Invalidates cache for particular space.
%% @end
%%-------------------------------------------------------------------
-spec invalidate_paths_caches(od_space:id()) -> ok.
invalidate_paths_caches(Space) ->
    ok = bounded_cache:invalidate(get_canonical_paths_cache_name(Space)),
    ok = bounded_cache:invalidate(get_uuid_based_paths_cache_name(Space)).


%%-------------------------------------------------------------------
%% @doc
%% Gets name of cache for particular space.
%% @end
%%-------------------------------------------------------------------
-spec get_canonical_paths_cache_name(od_space:id()) -> atom().
get_canonical_paths_cache_name(Space) ->
    ?CANONICAL_PATHS_CACHE_NAME(Space).

%%-------------------------------------------------------------------
%% @doc
%% Gets name of cache for particular space.
%% @end
%%-------------------------------------------------------------------
-spec get_uuid_based_paths_cache_name(od_space:id()) -> atom().
get_uuid_based_paths_cache_name(Space) ->
    ?UUID_BASED_PATHS_CACHE_NAME(Space).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init(od_space:id(), bounded_cache:cache()) -> ok.
init(Space, Name) ->
    try
        case bounded_cache:cache_exists(Name) of
            true ->
                ok;
            _ ->
                case bounded_cache:init_cache(Name, #{group => ?PATH_CACHE_GROUP}) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize paths bounded cache for space ~p due to: ~p",
                            [Space, Error])
                end
        end
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize paths bounded cache for space ~p due to: ~p",
                [Space, {Error2, Reason}])
    end.