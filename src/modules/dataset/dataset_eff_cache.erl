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
-include("modules/fslogic/data_access_control.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/1, init_group/0, invalidate_on_all_nodes/1]).
-export([get/1, get_eff_ancestor_datasets/1, get_eff_protection_flags/1]).
-compile([{no_auto_import, [get/1]}]).


%% RPC API
-export([invalidate/1]).

-define(CACHE_GROUP, <<"dataset_effective_cache_group">>).
-define(CACHE_NAME(SpaceId),
    binary_to_atom(<<"dataset_effective_cache_", SpaceId/binary>>, utf8)).

-define(CACHE_SIZE, application:get_env(?APP_NAME, dataset_eff_cache_size, 65536)).
-define(CHECK_FREQUENCY, application:get_env(?APP_NAME, dataset_check_frequency, 30000)).
-define(CACHE_OPTS, #{group => ?CACHE_GROUP}).

-record(entry, {
    direct_attached_dataset :: undefined | dataset:id(),
    eff_ancestor_datasets = [] :: [dataset:id()],
    eff_protection_flags :: data_access_control:bitmask()
}).

-type entry() :: #entry{}.
-type error() :: {error, term()}.


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
            ?debug("Unable to initialize datasets effective cache due to: ~p", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize datasets effective cache due to: ~p", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize datasets effective cache due to: ~p", [Error])
    catch
        Error2:Reason ->
            ?critical_stacktrace("Unable to initialize datasets effective cache due to: ~p", [{Error2, Reason}])
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
            ?error("Invalidation of datasets effective cache for space ~p failed on nodes: ~p (RPC error)", [SpaceId, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of datasets effective cache for space ~p failed.~n"
                "Reason: ~p", [SpaceId, Error]
            )
    end, Res).


-spec get_eff_ancestor_datasets(entry() | file_meta:doc()) -> {ok, [dataset_api:id()]} | error().
get_eff_ancestor_datasets(#entry{eff_ancestor_datasets = EffAncestorDatasets}) ->
    {ok, EffAncestorDatasets};
get_eff_ancestor_datasets(FileDoc) ->
    case get(FileDoc) of
        {ok, Entry} ->
            get_eff_ancestor_datasets(Entry);
        {error, _} = Error ->
            Error
    end.


-spec get_eff_protection_flags(entry() | file_meta:doc()) -> {ok, data_access_control:bitmask()} | error().
get_eff_protection_flags(#entry{eff_protection_flags = EffProtectionFlags}) ->
    {ok, EffProtectionFlags};
get_eff_protection_flags(FileDoc) ->
    case get(FileDoc) of
        {ok, Entry} ->
            get_eff_protection_flags(Entry);
        {error, _} = Error ->
            Error
    end.


-spec get(file_meta:doc()) -> {ok, entry()} | error().
get(FileDoc = #document{key = FileUuid}) ->
    case fslogic_uuid:is_root_dir_uuid(FileUuid) orelse fslogic_uuid:is_share_root_dir_uuid(FileUuid) of
        true ->
            {ok, #entry{eff_protection_flags = ?no_flags_mask}};
        false ->
            {ok, SpaceId} = file_meta:get_scope_id(FileDoc),
            CacheName = ?CACHE_NAME(SpaceId),
            Callback = fun([Doc, ParentEntry, CalculationInfo]) ->
                {ok, calculate(Doc, ParentEntry), CalculationInfo}
            end,
            MergeCallback = fun(NewEntry, EntryAcc, CalculationInfo) ->
                {ok, merge_entries_of_references(NewEntry, EntryAcc), CalculationInfo}
            end,
            Options = #{multi_path_merge_callback => MergeCallback},

            case effective_value:get_or_calculate(CacheName, FileDoc, Callback, Options) of
                {ok, Entry, _} ->
                    {ok, Entry};
                {error, {file_meta_missing, _}} ->
                    ?ERROR_NOT_FOUND
            end
    end.

%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id()) -> ok.
invalidate(SpaceId) ->
    ok = effective_value:invalidate(?CACHE_NAME(SpaceId)),
    ok = permissions_cache:invalidate_on_node().

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec calculate(file_meta:doc(), entry() | undefined) -> entry().
calculate(Doc = #document{}, undefined) ->
    % space dir as parent entry is undefined
    #entry{
        direct_attached_dataset = file_meta_dataset:get_id_if_attached(Doc),
        eff_ancestor_datasets = [],
        eff_protection_flags = get_protection_flags_if_dataset_attached(Doc)
    };
calculate(Doc = #document{}, #entry{
    direct_attached_dataset = ParentDirectAttachedDataset,
    eff_ancestor_datasets = ParentEffAncestorDatasets,
    eff_protection_flags = ParentEffProtectionFlags
}) ->
    EffAncestorDatasets = case ParentDirectAttachedDataset =/= undefined of
        true -> [ParentDirectAttachedDataset | ParentEffAncestorDatasets];
        false -> ParentEffAncestorDatasets
    end,
    ProtectionFlags = get_protection_flags_if_dataset_attached(Doc),
    #entry{
        direct_attached_dataset = file_meta_dataset:get_id_if_attached(Doc),
        eff_ancestor_datasets = EffAncestorDatasets,
        eff_protection_flags = ?set_flags(ParentEffProtectionFlags, ProtectionFlags)
    }.

-spec merge_entries_of_references(entry(), entry() | undefined) -> entry().
merge_entries_of_references(NewerEntry, undefined) ->
    NewerEntry;
merge_entries_of_references(#entry{
    eff_protection_flags = EffProtectionFlags2
} = _NewerEntry, #entry{
    direct_attached_dataset = DirectAttachedDataset1,
    eff_ancestor_datasets = EffAncestorDatasets1,
    eff_protection_flags = EffProtectionFlags1
} = _OlderEntry) ->
    % Reference for which effective_value:get_or_calculate is called is always calculated first.
    % We want to keep information about dataset calculated using only this reference and protection flags
    % calculated using all references.
    #entry{
        direct_attached_dataset = DirectAttachedDataset1,
        eff_ancestor_datasets = EffAncestorDatasets1,
        eff_protection_flags = ?set_flags(EffProtectionFlags1, EffProtectionFlags2)
    }.


-spec get_protection_flags_if_dataset_attached(file_meta:doc()) -> data_access_control:bitmask().
get_protection_flags_if_dataset_attached(FileDoc) ->
    case file_meta_dataset:is_attached(FileDoc) of
        true -> file_meta:get_protection_flags(FileDoc);
        false -> ?no_flags_mask
    end.