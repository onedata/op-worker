%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for effective checking recall status in 
%%% file ancestors. 
%%% Uses `effective_cache` under the hood.
%%% TODO VFS-7412 refactor this module (duplicated code in other effective_ caches modules)
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_cache).
-author("Michał Stanisz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_group/0, init/1, invalidate_on_all_nodes/1]).
-export([get/2]).
%% RPC API
-export([invalidate/1]).

-define(CACHE_GROUP, <<"archive_recall_cache_group">>).
-define(CACHE_NAME(SpaceId), binary_to_atom(<<"archive_recall_cache_", SpaceId/binary>>, utf8)).

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
            ?debug("Unable to initialize archive recall caches due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize archive recall caches due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize archive recall caches due to: ~p", [Error])
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
                        ?critical("Unable to initialize archive recall effective cache for space ~p due to: ~p",
                            [SpaceId, Error])
                end
        end
    catch
        Error2:Reason:Stacktrace ->
            ?critical_stacktrace("Unable to initialize archive recall effective cache for space ~p due to: ~p",
                [SpaceId, {Error2, Reason}], Stacktrace)
    end.


-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, invalidate, [SpaceId]),
    
    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Invalidation of archive recall caches for space ~p failed on nodes: ~p (RPC error)", [SpaceId, BadNodes])
    end,
    
    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of archive recall caches for space ~p failed.~n"
                "Reason: ~p", [SpaceId, Error]
            )
    end, Res).


-spec get(od_space:id(), file_meta:uuid() | file_meta:doc()) ->
    {ok, undefined | {ongoing | finished, file_meta:uuid()}} 
    | {error, {file_meta_missing, file_meta:uuid()}} | {error, term()}.
get(SpaceId, Doc = #document{value = #file_meta{}}) ->
    CacheName = ?CACHE_NAME(SpaceId),
    case effective_value:get_or_calculate(CacheName, Doc, fun calculate_archive_recall_status/1) of
        {ok, Res, _} ->
            {ok, Res};
        {error, _} = Error ->
            Error
    end;
get(SpaceId, Uuid) ->
    case file_meta:get_including_deleted(Uuid) of
        {ok, Doc} -> get(SpaceId, Doc);
        ?ERROR_NOT_FOUND -> {error, {file_meta_missing, Uuid}};
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ok = effective_value:invalidate(?CACHE_NAME(SpaceId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Calculates recall status along with uuid of root file of closest recall. 
%% When parent status is ongoing there is no need of further calculation, 
%% as it is impossible to create a recall in already recalling directory. 
%% When parent status is finished calculates further down, as there could 
%% be another recall (which will be closer).
%% @end
%%-------------------------------------------------------------------
-spec calculate_archive_recall_status(effective_value:args()) -> 
    {ok, undefined | {ongoing | finished, file_meta:uuid()}, effective_value:calculation_info()} 
    | {error, term()}.
calculate_archive_recall_status([_, {error, _} = Error, _CalculationInfo]) ->
    Error;
calculate_archive_recall_status([_, {ongoing, _} = ParentValue, CalculationInfo]) ->
    {ok, ParentValue, CalculationInfo};
calculate_archive_recall_status([#document{} = FileMetaDoc, ParentValue, CalculationInfo]) ->
    #document{key = FileUuid} = FileMetaDoc,
    case archive_recall:get_details(FileUuid) of
        {ok, #archive_recall_details{finish_timestamp = undefined}} -> 
            {ok, {ongoing, FileUuid}, CalculationInfo};
        {ok, #archive_recall_details{}} -> 
            {ok, {finished, FileUuid}, CalculationInfo};
        {error, not_found} -> 
            {ok, ParentValue, CalculationInfo};
        {error, _} = Error -> 
            Error
    end.
