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
-export([is_attached/1, is_attached/2]).
-export([get/2, get_eff_datasets/1, get_eff_datasets/2]).

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
    is_attached :: boolean(),
    eff_datasets = [] :: [dataset_api:id()]
}).

-type key() :: file_meta:uuid() | file_meta:doc().
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


-spec get(od_space:id(), key()) -> {ok, summary()} | error().
get(SpaceId, Doc = #document{}) ->
    CacheName = ?CACHE_NAME(SpaceId),
    Callback = fun([Doc, ParentSummary, CalculationInfo]) ->
        {ok, calculate_dataset_summary(Doc, ParentSummary), CalculationInfo}
    end,
    case effective_value:get_or_calculate(CacheName, Doc, Callback) of
        {ok, Summary, _} ->
            {ok, Summary};
        {error, {file_meta_missing, _}} ->
            ?ERROR_NOT_FOUND
    end;
get(SpaceId, Uuid) ->
    case file_meta:get(Uuid) of
        {ok, Doc} ->
            get(SpaceId, Doc);
        {error, _} = Error ->
            Error
    end.


-spec is_attached(od_space:id(), key()) -> {ok, boolean() | error()}.
is_attached(SpaceId, UuidOrDoc) ->
    case get(SpaceId, UuidOrDoc) of
        {ok, Summary} ->
            is_attached(Summary);
        {error, _} = Error ->
            Error
    end.

is_attached(#summary{is_attached = IsAttached}) ->
    {ok, IsAttached}.


-spec get_eff_datasets(od_space:id(), key()) -> {ok, [dataset_api:id()]} | error().
get_eff_datasets(SpaceId, UuidOrDoc) ->
    case get(SpaceId, UuidOrDoc) of
        {ok, Summary} ->
            get_eff_datasets(Summary);
        {error, _} = Error ->
            Error
    end.

get_eff_datasets(#summary{eff_datasets = EffDatasets}) ->
    {ok, EffDatasets}.


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


-spec calculate_dataset_summary(file_meta:doc(), summary() | undefined) -> summary().
calculate_dataset_summary(Doc = #document{}, undefined) ->
    IsDatasetAttached = file_meta:is_dataset_attached(Doc),
    EffDatasets = case IsDatasetAttached of
        false -> [];
        true -> [file_meta:get_dataset(Doc)]
    end,
    ?alert("Calc1: ~p", [    #summary{
        is_attached = IsDatasetAttached,
        eff_datasets = EffDatasets
    }]),
    #summary{
        is_attached = IsDatasetAttached,
        eff_datasets = EffDatasets
    };
calculate_dataset_summary(Doc = #document{}, #summary{
    eff_datasets = ParentEffDatasets
}) ->
    Dataset = file_meta:get_dataset(Doc),
    IsDatasetAttached = file_meta:is_dataset_attached(Doc),
    EffDatasets = case IsDatasetAttached of
        true -> [Dataset | ParentEffDatasets];
        false -> ParentEffDatasets
    end,

    ?alert("Calc2: ~p", [    #summary{
        is_attached = IsDatasetAttached,
        eff_datasets = EffDatasets
    }]),
    #summary{
        is_attached = IsDatasetAttached,
        eff_datasets = EffDatasets
    }.