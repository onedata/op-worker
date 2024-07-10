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
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/1, init_group/0, invalidate_on_all_nodes/1, invalidate_on_all_nodes/2]).
-export([get/1, get_eff_ancestor_datasets/1, get_eff_dataset_protection_flags/1, get_eff_protection_flags/1]).
-compile([{no_auto_import, [get/1]}]).


%% RPC API
-export([invalidate/2]).

-define(CACHE_GROUP, <<"dataset_effective_cache_group">>).
-define(CACHE_NAME(SpaceId),
    binary_to_atom(<<"dataset_effective_cache_", SpaceId/binary>>, utf8)).

-define(CACHE_SIZE, op_worker:get_env(dataset_eff_cache_size, 65536)).
-define(CHECK_FREQUENCY, op_worker:get_env(dataset_check_frequency, 30000)).
-define(CACHE_OPTS, #{group => ?CACHE_GROUP}).

-define(INVALIDATE_ON_DATASETS_GET, invalidate_on_datasets_get).

-record(entry, {
    direct_attached_dataset :: undefined | dataset:id(),
    eff_ancestor_datasets = [] :: [dataset:id()],
    % protection flags inherited only from ancestor datasets
    eff_dataset_protection_flags = ?no_flags_mask :: data_access_control:bitmask(),
    % all protection flags inherited from ancestor datasets as well as from all hardlinks' datasets
    eff_protection_flags = ?no_flags_mask :: data_access_control:bitmask()
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
        worker => false
    }).


-spec init(od_space:id() | all) -> ok.
init(all) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            lists:foreach(fun init/1, SpaceIds);
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            ?debug("Unable to initialize datasets effective cache due to: ~tp", [?ERROR_NO_CONNECTION_TO_ONEZONE]);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            ?debug("Unable to initialize datasets effective cache due to: ~tp", [?ERROR_UNREGISTERED_ONEPROVIDER]);
        Error = {error, _} ->
            ?critical("Unable to initialize datasets effective cache due to: ~tp", [Error])
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception("Unable to initialize datasets effective cache", Class, Reason, Stacktrace)
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
                        ?critical("Unable to initialize datasets effective cache for space ~tp due to: ~tp",
                            [SpaceId, Error])
                end
        end
    catch
        Class:Reason:Stacktrace ->
            ?critical_exception(
                "Unable to initialize datasets effective cache for space ~tp", [SpaceId],
                Class, Reason, Stacktrace
            )
    end.


-spec invalidate_on_all_nodes(od_space:id()) -> ok.
invalidate_on_all_nodes(SpaceId) ->
    invalidate_on_all_nodes(SpaceId, false).


-spec invalidate_on_all_nodes(od_space:id(), boolean()) -> ok.
invalidate_on_all_nodes(SpaceId, DatasetsOnly) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, invalidate, [SpaceId, DatasetsOnly]),

    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Invalidation of datasets effective cache for space ~tp failed on nodes: ~tp (RPC error)", [SpaceId, BadNodes])
    end,

    lists:foreach(fun
        (ok) -> ok;
        ({badrpc, _} = Error) ->
            ?error(
                "Invalidation of datasets effective cache for space ~tp failed.~n"
                "Reason: ~tp", [SpaceId, Error]
            )
    end, Res).


-spec get_eff_ancestor_datasets(entry() | file_meta:doc()) -> {ok, [dataset:id()]} | error().
get_eff_ancestor_datasets(#entry{eff_ancestor_datasets = EffAncestorDatasets}) ->
    {ok, EffAncestorDatasets};
get_eff_ancestor_datasets(FileDoc) ->
    case get(FileDoc) of
        {ok, Entry} ->
            get_eff_ancestor_datasets(Entry);
        {error, _} = Error ->
            Error
    end.


-spec get_eff_dataset_protection_flags(entry() | file_meta:doc()) -> {ok, data_access_control:bitmask()} | error().
get_eff_dataset_protection_flags(#entry{eff_dataset_protection_flags = EffProtectionFlags}) ->
    {ok, EffProtectionFlags};
get_eff_dataset_protection_flags(FileDoc) ->
    case get(FileDoc, false) of
        {ok, Entry} ->
            get_eff_dataset_protection_flags(Entry);
        {error, _} = Error ->
            Error
    end.


-spec get_eff_protection_flags(entry() | file_meta:doc()) -> {ok, data_access_control:bitmask()} | error().
get_eff_protection_flags(#entry{eff_protection_flags = EffProtectionFlags}) ->
    {ok, EffProtectionFlags};
get_eff_protection_flags(FileDoc) ->
    case get(FileDoc, false) of
        {ok, Entry} ->
            get_eff_protection_flags(Entry);
        {error, _} = Error ->
            Error
    end.


-spec get(file_meta:doc()) -> {ok, entry()} | error().
get(FileDoc) ->
    get(FileDoc, true).


-spec get(file_meta:doc(), boolean()) -> {ok, entry()} | error().
get(FileDoc, true = _CheckInvalidateOnDatasetsGetFlag) ->
    {ok, SpaceId} = file_meta:get_scope_id(FileDoc),
    case effective_value:get(?CACHE_NAME(SpaceId), ?INVALIDATE_ON_DATASETS_GET) of
        {ok, true} -> invalidate(SpaceId, false);
        _ -> ok
    end,

    get(FileDoc, false);
get(FileDoc = #document{key = FileUuid}, false = _CheckInvalidateOnDatasetsGetFlag) ->
    case fslogic_file_id:is_root_dir_uuid(FileUuid) orelse fslogic_file_id:is_share_root_dir_uuid(FileUuid) of
        true ->
            {ok, #entry{}};
        false ->
            {ok, SpaceId} = file_meta:get_scope_id(FileDoc),
            CacheName = ?CACHE_NAME(SpaceId),
            Callback = fun([Doc, ParentEntry, CalculationInfo]) ->
                {ok, calculate(Doc, ParentEntry), CalculationInfo}
            end,
            MergeCallback = fun(NewEntry, EntryAcc, _EntryCalculationInfo, CalculationInfoAcc) ->
                {ok, merge_entries_file_protection_flags(NewEntry, EntryAcc), CalculationInfoAcc}
            end,
            DifferentiateCallback = fun(ReferenceEntry, MergedEntry, _CalculationInfo) ->
                {ok, prepare_entry_to_cache(ReferenceEntry, MergedEntry)}
            end,
            Options = #{
                merge_callback => MergeCallback,
                differentiate_callback => DifferentiateCallback
            },

            case effective_value:get_or_calculate(CacheName, FileDoc, Callback, Options) of
                {ok, Entry, _} ->
                    {ok, Entry};
                {error, ?MISSING_FILE_META(_)} ->
                    ?ERROR_NOT_FOUND
            end
    end.

%%%===================================================================
%%% RPC API functions
%%%===================================================================

-spec invalidate(od_space:id(), boolean()) -> ok.
invalidate(SpaceId, false = _DatasetsOnly) ->
    ok = effective_value:invalidate(?CACHE_NAME(SpaceId)),
    ok = permissions_cache:invalidate_on_node();
invalidate(SpaceId, true = _DatasetsOnly) ->
    effective_value:cache(?CACHE_NAME(SpaceId), ?INVALIDATE_ON_DATASETS_GET, true).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec calculate(file_meta:doc(), entry() | undefined) -> entry().
calculate(Doc = #document{}, undefined) ->
    % space dir as parent entry is undefined
    ProtectionFlags = get_protection_flags_if_dataset_attached(Doc),
    #entry{
        direct_attached_dataset = file_meta_dataset:get_id_if_attached(Doc),
        eff_ancestor_datasets = [],
        eff_dataset_protection_flags = ProtectionFlags,
        eff_protection_flags = ProtectionFlags
    };
calculate(#document{key = ?ARCHIVES_ROOT_DIR_UUID(SpaceId), scope = SpaceId}, _) ->
    % files in archives cannot be established as datasets nor should they inherit dataset membership
    #entry{};
calculate(#document{key = ?TRASH_DIR_UUID(SpaceId), scope = SpaceId}, _) ->
    % files in archives cannot be established as datasets nor should they inherit dataset membership
    #entry{};
calculate(Doc = #document{}, #entry{
    direct_attached_dataset = ParentDirectAttachedDataset,
    eff_ancestor_datasets = ParentEffAncestorDatasets,
    eff_dataset_protection_flags = ParentEffDatasetProtectionFlags,
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
        eff_dataset_protection_flags = ?set_flags(ParentEffDatasetProtectionFlags, ProtectionFlags),
        eff_protection_flags = ?set_flags(ParentEffProtectionFlags, ProtectionFlags)
    }.

-spec merge_entries_file_protection_flags(entry(), entry()) -> entry().
merge_entries_file_protection_flags(#entry{
    eff_protection_flags = EffProtectionFlags1
} = _Entry, #entry{
    eff_protection_flags = EffProtectionFlags2
} = _EntryAcc) ->
    % Only protection flags are calculated together for all references
    #entry{
        eff_protection_flags = ?set_flags(EffProtectionFlags1, EffProtectionFlags2)
    }.

-spec prepare_entry_to_cache(entry(), entry()) -> entry().
prepare_entry_to_cache(ReferenceEntry, #entry{
    eff_protection_flags = EffProtectionFlags
} = _MergedEntry) ->
    ReferenceEntry#entry{eff_protection_flags = EffProtectionFlags}.


-spec get_protection_flags_if_dataset_attached(file_meta:doc()) -> data_access_control:bitmask().
get_protection_flags_if_dataset_attached(FileDoc) ->
    case file_meta_dataset:is_attached(FileDoc) of
        true -> file_meta:get_protection_flags(FileDoc);
        false -> ?no_flags_mask
    end.