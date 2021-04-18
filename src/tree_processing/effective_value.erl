%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides ets cache for effective values.
%%% It is based on bounded_cache mechanism (see bounded_cache.erl
%%% in cluster_worker). Cache is cleaned automatically when defined size
%%% is exceeded (size is checked periodically).
%%% It allows calculation of value recursively (from file/dir to space)
%%% caching final and intermediate results for better performance.
%%%
%%% The module allows recursive calculation of value basing on single
%%% reference of file or all file's references. Three modes are possible:
%%%    - calculation only for single reference (no additional options needed),
%%%    - calculation using all references where final value is aggregated
%%%      using values calculated for all references - merge_callback has to
%%%      be provided for reference values` aggregation
%%%    - calculation using all references where final value is calculated using
%%%      values of multiple references but can be different for different
%%%      references - merge_callback has to  be provided for reference values`
%%%      aggregation and differentiate_callback has to be provider to calculate
%%%      final value of the reference using value calculated using ancestors of
%%%      this reference and value aggregated from all references by merge_callback.
%%% @end
%%%-------------------------------------------------------------------
-module(effective_value).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_cache/2, cache_exists/1, invalidate/1]).
-export([init_group/2]).
-export([get_or_calculate/3, get_or_calculate/4]).

-type cache() :: bounded_cache:cache().
-type init_options() :: bounded_cache:cache_options().
-type group() :: bounded_cache:group().
-type group_options() :: bounded_cache:group_options().
-type calculation_info() :: term(). % Function that calculates value returns additional information
                                    % (CalculationInfo) that can be useful for further work
                                    % (e.g., calculating function can include datastore documents getting and
                                    % these documents can be used later without calling datastore).
                                    % Such returned value is provided to calculate function when processing
                                    % child in case of recursive value calculation
                                    % (see bounded_cache in cluster_worker repository)).
-type initial_calculation_info() :: calculation_info(). % This type represents initial value provided to function
                                                        % when processing first item.
-type args() :: list().
-type in_critical_section() :: boolean() | parent. % parent = use section starting from parent directory
% Type that defines return value of get_or_calculate functions and helper functions (used to shorten specs)
-type get_return_value() :: {ok, bounded_cache:value(), calculation_info()} | {error, term()}.
% Merge callback is used to merge values calculated using different references of file.
% If it is not present, only reference pointing at file doc passed by get_or_calculate function argument is used.
-type merge_callback() :: fun((Ref1Value :: bounded_cache:value(), Ref2Value :: bounded_cache:value(),
    Ref1CalculationInfo :: calculation_info(), RefCalculationInfo :: calculation_info()) -> get_return_value()).
% Callback that allows caching of different values for different references of the same file during single call.
% It uses value calculated for reference and value provided by merge_callback to obtain final value for particular
% reference.
-type differentiate_callback() :: fun((RefValue :: bounded_cache:value(), MergedValue :: bounded_cache:value(),
    calculation_info()) -> {ok, bounded_cache:value()} | {error, term()}).
-type get_options() :: #{
    timestamp => time:millis(),
    in_critical_section => in_critical_section(),
    initial_calculation_info => initial_calculation_info(),
    args => args(),
    use_referenced_key => boolean(), % use referenced key to find/cache value instead of key of file doc
                                     % passed by get_or_calculate function argument
    merge_callback => merge_callback(), % note: use `referenced_key = true` for more optimal caching
                                        % if differentiate_callback is not used
    differentiate_callback => differentiate_callback(), % note: do not use together with `referenced_key = true`
    force_execution_on_inode => boolean() % force execution of callback on inode even if reference of original file
                                          % is deleted
}.

-export_type([cache/0, get_options/0]).
-define(CRITICAL_SECTION(Cache, Key), {effective_value_insert, Cache, Key}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init_cache(cache(), init_options()) -> ok | {error, term()}.
init_cache(Cache, CacheOptions) ->
    bounded_cache:init_cache(Cache, CacheOptions).


-spec init_group(group(), group_options()) -> ok | {error, term()}.
init_group(Group, Options) ->
    bounded_cache:init_group(Group, Options).


-spec cache_exists(cache()) -> boolean().
cache_exists(Cache) ->
    bounded_cache:cache_exists(Cache).


-spec invalidate(bounded_cache:cache()) -> ok.
invalidate(Cache) ->
    bounded_cache:invalidate(Cache).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_or_calculate(Cache, FileDoc, CalculateCallback, #{})
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback()) -> get_return_value().
get_or_calculate(Cache, FileDoc, CalculateCallback) ->
    get_or_calculate(Cache, FileDoc, CalculateCallback, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Gets value from cache. If it is not found - uses callback to calculate it.
%% Calculated value is cached. Besides calculated value function returns additional information (CalculationInfo)
%% that is generated by calculate function and can be useful for further work
%% (e.g., calculating function can include datastore documents getting - see bounded_cache.erl in cluster_worker).
%% Calculate function processes single argument that is list [Doc, ParentValue, CalculationInfo | Args] where Doc is
%% file/directory file_meta document while ParentValue and CalculationInfo are results of calling this function on
%% parent. Function is called recursively starting from space document. ParentValue and CalculationInfo are set to
%% undefined and InitialCalculationInfo for space document (it has no parent).
%% Note: it is possible to calculate value using all references of file - see main doc.
%% @end
%%--------------------------------------------------------------------
-spec get_or_calculate(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(), get_options()) ->
    get_return_value().
get_or_calculate(Cache, #document{key = DocKey} = FileDoc, CalculateCallback, Options) ->
    % use_referenced_key option should be used only for file for which function is called, set false for ancestors
    {Key, Options2} = case maps:get(use_referenced_key, Options, false) of
        true -> {fslogic_uuid:ensure_referenced_uuid(DocKey), Options#{use_referenced_key => false}};
        false -> {DocKey, Options}
    end,

    % Timestamp is required by internal functions - add it if it is undefined
    Options3 = case maps:get(timestamp, Options2, undefined) of
        undefined -> Options2#{timestamp => bounded_cache:get_timestamp()};
        _ -> Options2
    end,

    case maps:get(in_critical_section, Options3, false) of
        true -> get_or_calculate_in_critical_section(Cache, Key, FileDoc, CalculateCallback, Options3);
        false -> get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options3);
        % Use critical section for parent directory (changed option will be used in recursive call)
        parent -> get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options3#{in_critical_section => true})
    end.

%%%===================================================================
%%% get_or_calculate - internal functions
%%%===================================================================

-spec get_or_calculate_in_critical_section(bounded_cache:cache(), file_meta:uuid(), file_meta:doc(),
    bounded_cache:callback(), get_options()) -> get_return_value().
get_or_calculate_in_critical_section(Cache, Key, FileDoc, CalculateCallback, Options) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, maps:get(initial_calculation_info, Options, undefined)};
        {error, not_found} ->
            critical_section:run(?CRITICAL_SECTION(Cache, Key), fun() ->
                get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options)
            end)
    end.

-spec get_or_calculate_internal(bounded_cache:cache(), file_meta:uuid(), file_meta:doc(), bounded_cache:callback(),
    get_options()) -> get_return_value().
get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options) ->
    % Note - argument Key and field key if FileDoc can differ - see use_referenced_key option
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, maps:get(initial_calculation_info, Options, undefined)};
        {error, not_found} ->
            MergeCallback = maps:get(merge_callback, Options, undefined),
            MultiRefs = case {MergeCallback, file_meta:get_effective_type(FileDoc)} of
                {undefined, _} -> false; % merge_callback is undefined - calculate only for single reference
                {_, ?REGULAR_FILE_TYPE} -> true; % only reg files and hardlinks can have multiple references
                _ -> false
            end,

            case {fslogic_uuid:is_space_dir_uuid(Key), fslogic_uuid:is_root_dir_uuid(Key), MultiRefs} of
                {false, false, false} ->
                    get_or_calculate_single_reference(Cache, Key, FileDoc, CalculateCallback, Options);
                {false, false, true} ->
                    get_or_calculate_multiple_references(Cache, Key, FileDoc, CalculateCallback, Options);
                {false, true, _} ->
                    ?critical("Incorrect usage of effective_value cache ~p. "
                        "Calculation has reached the global root directory.", [Cache]),
                    {error, root_dir_reached};
                {true, _, _} ->
                    % root of space - init calculation with parent value undefined
                    Args = maps:get(args, Options, []),
                    Timestamp = maps:get(timestamp, Options),
                    InitialCalculationInfo = maps:get(initial_calculation_info, Options, undefined),
                    bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                        [FileDoc, undefined, InitialCalculationInfo | Args], Timestamp)
            end
    end.

-spec get_or_calculate_single_reference(bounded_cache:cache(), file_meta:uuid(), file_meta:doc(),
    bounded_cache:callback(), get_options()) -> get_return_value().
get_or_calculate_single_reference(Cache, Key, FileDoc, CalculateCallback, Options) ->
    case calculate_for_parent(Cache, Key, FileDoc, CalculateCallback, Options) of
        {ok, ParentValue, CalculationInfo} ->
            Args = maps:get(args, Options, []),
            Timestamp = maps:get(timestamp, Options),
            bounded_cache:calculate_and_cache(Cache, Key, CalculateCallback,
                [FileDoc, ParentValue, CalculationInfo | Args], Timestamp);
        {error, _} = Error ->
            Error
    end.

-spec get_or_calculate_multiple_references(bounded_cache:cache(), file_meta:uuid(), file_meta:doc(),
    bounded_cache:callback(), get_options()) -> get_return_value().
get_or_calculate_multiple_references(Cache, Key, #document{key = DocKey} = FileDoc, CalculateCallback, Options) ->
    MergeCallback = maps:get(merge_callback, Options),
    References = get_references(FileDoc),
    ReferencesValues = lists:map(fun(ReferenceDoc) ->
        calculate_reference(Cache, ReferenceDoc, CalculateCallback, Options)
    end, References),

    case merge_references_values(ReferencesValues, undefined, MergeCallback) of
        {ok, Acc, CalculationInfoAcc} = OkAns ->
            {ok, CalculatedValue, CalculationInfo} = case maps:get(force_execution_on_inode, Options, false) of
                true ->
                    INodeKey = fslogic_uuid:ensure_referenced_uuid(Key),
                    case lists:member(INodeKey, [DocKey | References]) of
                        true ->
                            OkAns;
                        false ->
                            force_execution_on_inode(
                                INodeKey, CalculateCallback, MergeCallback, CalculationInfoAcc, Acc, Options)
                    end;
                false ->
                    OkAns
            end,

            DifferentiateCallback = maps:get(differentiate_callback, Options, undefined),
            Timestamp = maps:get(timestamp, Options),
            differentiate_and_cache_references(Cache, Key, CalculatedValue, CalculationInfo,
                References, ReferencesValues, Timestamp, DifferentiateCallback);
        Error ->
            Error
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec calculate_for_parent(bounded_cache:cache(), file_meta:uuid(), file_meta:doc(), bounded_cache:callback(),
    get_options()) -> get_return_value().
calculate_for_parent(Cache, Key, FileDoc, CalculateCallback, Options) ->
    {ok, ParentUuid} = get_parent(Key, FileDoc),
    case file_meta:get_including_deleted(ParentUuid) of
        {ok, ParentDoc} -> get_or_calculate(Cache, ParentDoc, CalculateCallback, Options);
        _ -> {error, {file_meta_missing, ParentUuid}}
    end.

-spec calculate_reference(bounded_cache:cache(), file_meta:doc(), bounded_cache:callback(), get_options()) ->
    get_return_value().
calculate_reference(Cache, #document{key = Key} = FileDoc, CalculateCallback, Options) ->
    case calculate_for_parent(Cache, Key, FileDoc, CalculateCallback, Options) of
        {ok, ParentValue, ParentCalculationInfo} ->
            Args = maps:get(args, Options, []),
            CalculateCallback([FileDoc, ParentValue, ParentCalculationInfo | Args]);
        {error, _} = Error ->
            Error
    end.

-spec merge_references_values([get_return_value()], undefined | get_return_value(), merge_callback()) ->
    get_return_value().
merge_references_values([], Acc, _MergeCallback) ->
    Acc; % No references left - return answer
merge_references_values([Value | Tail], undefined, MergeCallback) ->
    merge_references_values(Tail, Value, MergeCallback); % First value - nothing to be merged
merge_references_values(_ReferenceValues, {error, _} = Acc, _MergeCallback) ->
    Acc; % Error occurred - return answer
merge_references_values([{ok, Value, CalculationInfo} | Tail], {ok, AccValue, AccCalculationInfo}, MergeCallback) ->
    merge_references_values(Tail, MergeCallback(AccValue, Value, AccCalculationInfo, CalculationInfo), MergeCallback).

-spec differentiate_and_cache_references(bounded_cache:cache(), file_meta:uuid(), bounded_cache:value(),
    calculation_info(), [file_meta:doc()], [get_return_value()], time:millis(), differentiate_callback()) ->
    get_return_value().
differentiate_and_cache_references(Cache, Key, MergedValue, CalculationInfo,
    _References, _ReferencesValues, Timestamp, undefined) ->
    bounded_cache:cache(Cache, Key, MergedValue, Timestamp),
    {ok, MergedValue, CalculationInfo};
differentiate_and_cache_references(Cache, _Key, MergedValue, CalculationInfo,
    References, ReferencesValues, Timestamp, DifferentiateCallback) ->
    % Apply callback for all references
    FoldlAns = lists:foldl(fun
        ({ok, ReferenceValue, _}, {ok, Acc}) ->
            case DifferentiateCallback(ReferenceValue, MergedValue, CalculationInfo) of
                {ok, NewValue} -> {ok, [NewValue | Acc]};
                Other -> Other
            end;
        (_, Error) ->
            Error
    end, {ok, []}, ReferencesValues),

    % Cache reference is all callback calls succeeded
    case FoldlAns of
        {ok, ReversedMappedReferencesValues} ->
            [ReturnValue | _] = MappedReferencesValues = lists:reverse(ReversedMappedReferencesValues),
            lists:foreach(fun({#document{key = CacheKey}, ValueToCache}) ->
                bounded_cache:cache(Cache, CacheKey, ValueToCache, Timestamp)
            end, lists:zip(References, MappedReferencesValues)),    
            
            {ok, ReturnValue, CalculationInfo};
        FoldlError ->
            FoldlError
    end.

-spec force_execution_on_inode(file_meta:uuid(), bounded_cache:callback(), merge_callback(),
    calculation_info(), bounded_cache:value(), get_options()) -> get_return_value().
force_execution_on_inode(INodeKey, CalculateCallback, MergeCallback, CalculationInfo, Acc, Options) ->
    case file_meta:get_including_deleted(INodeKey) of
        {ok, FileDoc} ->
            Args = maps:get(args, Options, []),
            InitialCalculationInfo = maps:get(initial_calculation_info, Options, undefined),
            case CalculateCallback([FileDoc, undefined, InitialCalculationInfo | Args]) of
                {ok, Value, NewCalculationInfo} -> MergeCallback(Acc, Value, CalculationInfo, NewCalculationInfo);
                {error, _} = Error -> Error
            end;
        _ ->
            {error, {file_meta_missing, INodeKey}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Function used to optimize getting parent doc. If Key is equal to key inside FileDoc,
%% FileDoc can be used. It results in one datastore get operation less.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(file_meta:uuid(), file_meta:doc()) -> {ok, file_meta:uuid()} | {error, term()}.
get_parent(Key, #document{key = Key} = FileDoc) ->
    file_meta:get_parent_uuid(FileDoc);
get_parent(Key, _FileDoc) ->
    % Key differs from key inside FileDoc (see use_referenced_key option) - file doc for Key 
    % will be get inside get_parent_uuid function
    file_meta:get_parent_uuid(Key).

-spec get_references(file_meta:doc()) -> [file_meta:doc()].
get_references(#document{key = DocKey} = FileDoc) ->
    %% @TODO VFS-7555 Use Doc for listing references after it is allowed
    {ok, References} = case fslogic_uuid:ensure_referenced_uuid(DocKey) of
        DocKey -> file_meta_hardlinks:list_references(FileDoc);
        ReferencedUuid -> file_meta_hardlinks:list_references(ReferencedUuid)
    end,

    [FileDoc | lists:filtermap(fun(Uuid) ->
        case file_meta:get(Uuid) of
            {ok, Doc} -> {true, Doc};
            {error, not_found} -> false
        end
    end, References -- [DocKey])].