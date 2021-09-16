%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for effective checking synchronization status of file_meta links. 
%%% Uses `effective_cache` under the hood.
%%% TODO VFS-7412 refactor this module (duplicated code in other effective_ caches modules)
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_links_sync_status_cache).
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init/1]).
-export([get/2]).

-define(CACHE_GROUP, <<"file_meta_links_sync_status_cache_group">>).
-define(CACHE_NAME(SpaceId),
    binary_to_atom(<<"file_meta_links_effective_cache_", SpaceId/binary>>, utf8)).

-define(CACHE_SIZE, op_worker:get_env(file_meta_links_eff_cache_size, 65536)).
-define(CHECK_FREQUENCY, op_worker:get_env(file_meta_links_cache_check_frequency, 30000)).
-define(CACHE_OPTS, #{group => ?CACHE_GROUP}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_group() -> ok.
init_group() ->
    ok = effective_value:init_group(?CACHE_GROUP, #{
        check_frequency => ?CHECK_FREQUENCY,
        size => ?CACHE_SIZE,
        worker => true
    }).


-spec init(od_space:id() | all) -> ok.
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize file_meta links caches due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize file_meta links caches due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize file_meta links caches due to: ~p", [Error])
    catch
        Error2:Reason:Stacktrace ->
            ?critical_stacktrace("Unable to initialize file_meta links caches due to: ~p", [{Error2, Reason}], Stacktrace)
    end;
init(SpaceId) ->
    CacheName = ?CACHE_NAME(SpaceId),
    try
        case effective_value:cache_exists(CacheName) of
            true ->
                ok;
            _ ->
                case effective_value:init_cache(CacheName, ?CACHE_OPTS) of
                    ok ->
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize file_meta links effective cache for space ~p due to: ~p",
                            [SpaceId, Error])
                end
        end
    catch
        Error2:Reason:Stacktrace ->
            ?critical_stacktrace("Unable to initialize file_meta links effective cache for space ~p due to: ~p",
                [SpaceId, {Error2, Reason}], Stacktrace)
    end.


-spec get(od_space:id(), file_meta:uuid() | file_meta:doc()) ->
    {ok, synced} | {error, {file_meta_missing, file_meta:uuid()}} | {error, {link_missing, file_meta:uuid()}}.
get(SpaceId, Doc = #document{value = #file_meta{}}) ->
    CacheName = ?CACHE_NAME(SpaceId),
    case effective_value:get_or_calculate(CacheName, Doc, calculate_links_sync_status()) of
        {ok, synced, _} ->
            {ok, synced};
        {error, _} = Error ->
            Error
    end;
get(SpaceId, Uuid) ->
    case file_meta:get_including_deleted(Uuid) of
        {ok, Doc} -> get(SpaceId, Doc);
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec calculate_links_sync_status() -> fun().
calculate_links_sync_status() ->
    fun([
        #document{
            value = #file_meta{name = Name, parent_uuid = ParentUuid}
        },
        _ParentValue, CalculationInfo
    ]) ->
        case file_meta_forest:get(ParentUuid, all, Name) of
            {ok, _} -> {ok, synced, CalculationInfo};
            {error, _} -> {error, {link_missing, ParentUuid}}
        end
    end.