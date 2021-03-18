%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Effective cache for tracking dataset membership of files.
%%% It allows to determine whether file belongs to dataset and
%%% what are its ancestor datasets.
%%% TODO VFS-7412 refactor this module (duplicated code in other effective_ caches modules)
%%% after refactoring effective_value
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_eff_cache).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/dataset.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/1, init_group/0, invalidate_on_all_nodes/1]).
-export([get_eff_ancestor_datasets/1]).
-compile([{no_auto_import, [get/1]}]).


%% RPC API
-export([invalidate/1]).

-define(CACHE_GROUP, <<"dataset_effective_cache_group">>).
-define(CACHE_NAME(SpaceId),
    binary_to_atom(<<"dataset_effective_cache_", SpaceId/binary>>, utf8)).

-define(CACHE_SIZE, application:get_env(?APP_NAME, dataset_eff_cache_size, 65536)).
-define(CHECK_FREQUENCY, application:get_env(?APP_NAME, dataset_check_frequency, 30000)).
-define(CACHE_OPTS, #{
    size => ?CACHE_SIZE,
    check_frequency => ?CHECK_FREQUENCY
}).

-record(summary, {
    direct_attached_dataset :: undefined | dataset:id(),
    eff_ancestor_datasets = [] :: [dataset:id()]
}).

-type summary() :: #summary{}.
-type error() :: {error, term()}.

% TODO zrobic grupę zeby ograniczas sumaryczn rozmiar?
% TODO startowac na wszystkich node'ach?
% TODO usunac dupliackej kodu z innymi modulami?

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
            ?debug("Unable to initialize datasets effective summary cache due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize datasets effective summary cache due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize datasets effective summary cache due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize datasets effective summary cache due to: ~p", [{Error2, Reason}])
    end;
init(SpaceId) ->
    ?alert("WIll init: ~p", [?CACHE_NAME(SpaceId)]),
    CacheName = ?CACHE_NAME(SpaceId),
    try
        case effective_value:cache_exists(CacheName) of
            true ->
                ?alert("EXISTS: ~p", [?CACHE_NAME(SpaceId)]),
                ok;
            _ ->
                case effective_value:init_cache(CacheName, ?CACHE_OPTS) of
                    ok ->
                        ?alert("INIT: ~p", [?CACHE_NAME(SpaceId)]),
                        ok;
                    Error = {error, _} ->
                        ?critical("Unable to initialize datasets effective cache for space ~p due to: ~p",
                            [SpaceId, Error])
                end
        end
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize datasets effective cache for space ~p due to: ~p",
                [SpaceId, {Error2, Reason}])
    end.


-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = rpc:multicall(Nodes, ?MODULE, invalidate, [SpaceId]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Invalidation of datasets effective summary cache for space ~p failed on nodes: ~p (RPC error)", [SpaceId, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of datasets effective summary cache for space ~p failed.~n"
                "Reason: ~p", [SpaceId, Error]
            )
    end, Res).


-spec get_eff_ancestor_datasets(file_meta:doc()) -> {ok, [dataset_api:id()]} | error().
get_eff_ancestor_datasets(FileDoc) ->
    case get(FileDoc) of
        {ok, #summary{eff_ancestor_datasets = EffAncestorDatasets}} ->
            {ok, EffAncestorDatasets};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ?alert("WILL INVALIDATE: ~p", [?CACHE_NAME(SpaceId)]),
    ok = effective_value:invalidate(?CACHE_NAME(SpaceId)).

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec get(file_meta:doc()) -> {ok, summary()} | error().
get(FileDoc) ->
    SpaceId = file_meta:get_scope_id(FileDoc),
    CacheName = ?CACHE_NAME(SpaceId),
    Callback = fun([Doc, ParentSummary, CalculationInfo]) ->
        {ok, calculate_dataset_summary(Doc, ParentSummary), CalculationInfo}
    end,
    case effective_value:get_or_calculate(CacheName, FileDoc, Callback) of
        {ok, Summary, _} ->
            {ok, Summary};
        {error, {file_meta_missing, _}} ->
            ?ERROR_NOT_FOUND
    end.


-spec calculate_dataset_summary(file_meta:doc(), summary() | undefined) -> summary().
calculate_dataset_summary(Doc = #document{}, undefined) ->
    #summary{
        direct_attached_dataset = get_direct_dataset_if_attached(Doc),
        eff_ancestor_datasets = []
    };
calculate_dataset_summary(Doc = #document{}, #summary{
    direct_attached_dataset = ParentDirectAttachedDataset,
    eff_ancestor_datasets = ParentEffAncestorDatasets
}) ->
    EffAncestorDatasets = case ParentDirectAttachedDataset =/= undefined of
        true -> [ParentDirectAttachedDataset | ParentEffAncestorDatasets];
        false -> ParentEffAncestorDatasets
    end,
    #summary{
        direct_attached_dataset = get_direct_dataset_if_attached(Doc),
        eff_ancestor_datasets = EffAncestorDatasets
    }.


-spec get_direct_dataset_if_attached(file_meta:doc()) -> dataset:id() | undefined.
get_direct_dataset_if_attached(FileDoc) ->
    IsDatasetAttached = file_meta:is_dataset_attached(FileDoc),
    case IsDatasetAttached of
        true -> file_meta:get_dataset(FileDoc);
        false -> undefined
    end.