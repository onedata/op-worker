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
%%%    - calculation using all references where final value is merged
%%%      using values calculated for all references - merge_callback has to
%%%      be provided for reference values` aggregation
%%%    - calculation using all references where final value is calculated using
%%%      values of multiple references but can be different for different
%%%      references - merge_callback has to  be provided for reference values`
%%%      aggregation and differentiate_callback has to be provider to calculate
%%%      final value of the reference using value calculated using ancestors of
%%%      this reference and value merged from all references by merge_callback.
%%% @end
%%%-------------------------------------------------------------------
-module(effective_value).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_cache/2, cache_exists/1, invalidate/1]).
-export([init_group/2]).
-export([get/2, cache/3, get_or_calculate/3, get_or_calculate/4]).

-type cache() :: bounded_cache:cache().
-type init_options() :: bounded_cache:cache_options().
-type group() :: bounded_cache:group().
-type group_options() :: bounded_cache:group_options().
% Function that calculates value returns additional information (CalculationInfo) that can be useful for further work
% (e.g., calculating function can include datastore documents getting and these documents can be used later without
% calling datastore). Such returned value is provided to calculate function when processing child in case of
% recursive value calculation (see bounded_cache in cluster_worker repository)).
-type calculation_info() :: bounded_cache:additional_info().
-type args() :: list().
-type critical_section_level() :: no | direct | parent. % parent = use section starting from parent directory
% Type that defines return value of get_or_calculate functions and helper functions (used to shorten specs)
-type get_or_calculate_return_value() :: {ok, bounded_cache:value(), calculation_info()} | {error, term()}.
-type callback() :: bounded_cache:callback().
% Merge callback is used to merge values calculated using different references of file.
% If it is not present, only reference pointing at file doc passed by get_or_calculate function argument is used.
-type merge_callback() :: fun((RefValue :: bounded_cache:value(), ValueAcc :: bounded_cache:value(),
    RefCalculationInfo :: calculation_info(), CalculationInfoAcc :: calculation_info()) -> get_or_calculate_return_value()).
% Callback that allows caching of different values for different references of the same file during single call.
% It uses value calculated for reference and value provided by merge_callback to obtain final value for particular
% reference.
-type differentiate_callback() :: fun((RefValue :: bounded_cache:value(), MergedValue :: bounded_cache:value(),
    calculation_info()) -> {ok, bounded_cache:value()} | {error, term()}).
-type get_or_calculate_options() :: #{
    timestamp => time:millis(),
    critical_section_level => critical_section_level(),
    initial_calculation_info => calculation_info(), % Represents initial value provided to function
                                                    % when processing first item.
    args => args(),
    use_referenced_key => boolean(), % use referenced key to find/cache value instead of key of file doc
                                     % passed by get_or_calculate function argument
    merge_callback => merge_callback() | undefined, % note: use `referenced_key = true` for more optimal caching
                                                    % if differentiate_callback is not used
    differentiate_callback => differentiate_callback(), % note: do not use together with `referenced_key = true`
    force_execution_on_referenced_key => boolean(), % force execution of callback on inode even if reference of original file
                                                    % is deleted
    calculation_root_parent => file_meta:uuid(), % default: <<>>; should be uuid of directory.
                                                 % NOTE: if not equal to <<>> values will NOT be cached
                                                 % NOTE: this option works in best effort manner - if there is already value
                                                 %       calculated from space root cached it will be returned.
    should_cache => boolean(), % default: true; indicates whether calculated value should be cached,
    get_remote_from_scope => od_space:id() % allow getting parent docs from remote providers if they do not exist
                                           % locally - see file_meta:get_including_deleted_local_or_remote for more information
}.

-export_type([args/0, calculation_info/0]).
-export_type([cache/0, callback/0, get_or_calculate_options/0]).
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


-spec invalidate(cache()) -> ok.
invalidate(Cache) ->
    bounded_cache:invalidate(Cache).

-spec get(cache(), term()) -> {ok, term()} | {error, not_found}.
get(Cache, Key) ->
    bounded_cache:get(Cache, Key).

-spec cache(cache(), term(), term()) -> ok.
cache(Cache, Key, Value) ->
    bounded_cache:cache(Cache, Key, Value, bounded_cache:get_timestamp()).

-spec get_or_calculate(cache(), file_meta:doc(), callback()) -> get_or_calculate_return_value().
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
-spec get_or_calculate(cache(), file_meta:doc(), callback(), get_or_calculate_options()) ->
    get_or_calculate_return_value().
get_or_calculate(Cache, #document{key = DocKey} = FileDoc, CalculateCallback, Options) ->
    % use_referenced_key option should be used only for file for which function is called, set false for ancestors
    {Key, Options2} = case maps:get(use_referenced_key, Options, false) of
        true -> {fslogic_file_id:ensure_referenced_uuid(DocKey), Options#{use_referenced_key => false}};
        false -> {DocKey, Options}
    end,

    % Set timestamp if it is undefined as effective_value cannot base on timestamps managed by bounded_cache
    % (calculation of effective value may include several calls to bounded_cache and all calls have to use
    % timestamp previous to first execution of CalculateCallback function)
    Options3 = case maps:get(timestamp, Options2, undefined) of
        undefined -> Options2#{timestamp => bounded_cache:get_timestamp()};
        _ -> Options2
    end,

    case maps:get(critical_section_level, Options3, no) of
        direct ->
            get_or_calculate_in_critical_section(Cache, Key, FileDoc, CalculateCallback, Options3);
        no ->
            get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options3);
        % Use critical section for parent directory (changed option will be used in recursive call)
        parent ->
            get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options3#{critical_section_level => direct})
    end.

%%%===================================================================
%%% get_or_calculate - internal functions
%%%===================================================================

-spec get_or_calculate_in_critical_section(cache(), file_meta:uuid(), file_meta:doc(),
    callback(), get_or_calculate_options()) -> get_or_calculate_return_value().
get_or_calculate_in_critical_section(Cache, Key, FileDoc, CalculateCallback, Options) ->
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, maps:get(initial_calculation_info, Options, undefined)};
        {error, not_found} ->
            critical_section:run(?CRITICAL_SECTION(Cache, Key), fun() ->
                get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options)
            end)
    end.

-spec get_or_calculate_internal(cache(), file_meta:uuid(), file_meta:doc(), callback(),
    get_or_calculate_options()) -> get_or_calculate_return_value().
get_or_calculate_internal(Cache, Key, FileDoc, CalculateCallback, Options) ->
    % Note - argument Key and field key if FileDoc can differ - see use_referenced_key option
    CalculationRootParent = maps:get(calculation_root_parent, Options, <<>>),
    case bounded_cache:get(Cache, Key) of
        {ok, Value} ->
            {ok, Value, maps:get(initial_calculation_info, Options, undefined)};
        {error, not_found} ->
            MergeCallback = maps:get(merge_callback, Options, undefined),
            % only reg files and hardlinks can have multiple references
            ShouldProcessMultipleRefs = (MergeCallback =/= undefined)
                andalso (file_meta:get_effective_type(FileDoc) =:= ?REGULAR_FILE_TYPE),

            ShouldCache = case maps:get(should_cache, Options, true) of
                true -> CalculationRootParent =:= <<>> orelse error(improper_use_of_effective_value);
                false -> false
            end,
            {ok, Parent} = file_meta:get_parent_uuid(FileDoc),
            case {CalculationRootParent =:= Parent, fslogic_file_id:is_root_dir_uuid(Key), ShouldProcessMultipleRefs} of
                {false, false, false} ->
                    calculate_single_reference(Cache, Key, FileDoc, CalculateCallback, Options, ShouldCache);
                {false, false, true} ->
                    get_or_calculate_multiple_references(Cache, Key, FileDoc, CalculateCallback, Options, ShouldCache);
                {false, true, _} ->
                    ?critical("Incorrect usage of effective_value cache ~p. "
                        "Calculation has reached the global root directory.", [Cache]),
                    {error, root_dir_reached};
                {true, _, _} ->
                    % calculation root - init calculation with parent value undefined
                    InitialCalculationInfo = maps:get(initial_calculation_info, Options, undefined),
                    calculate_and_maybe_cache(
                        Cache, Key, CalculateCallback, Options, [FileDoc, undefined, InitialCalculationInfo], ShouldCache)
            end
    end.

-spec calculate_single_reference(cache(), file_meta:uuid(), file_meta:doc(), callback(), get_or_calculate_options(),
    boolean()) -> get_or_calculate_return_value().
calculate_single_reference(Cache, Key, FileDoc, CalculateCallback, Options, ShouldCache) ->
    case calculate_for_parent(Cache, Key, FileDoc, CalculateCallback, Options) of
        {ok, ParentValue, ParentCalculationInfo} ->
            calculate_and_maybe_cache(
                Cache, Key, CalculateCallback, Options, [FileDoc, ParentValue, ParentCalculationInfo], ShouldCache);
        {error, _} = Error ->
            Error
    end.

-spec calculate_and_maybe_cache(cache(), file_meta:uuid(), callback(), get_or_calculate_options(), args(), boolean()) ->
    get_or_calculate_return_value().
calculate_and_maybe_cache(Cache, Key, CalculateCallback, Options, ArgsPrefix, ShouldCache) ->
    Args = ArgsPrefix ++ maps:get(args, Options, []),
    Timestamp = maps:get(timestamp, Options),
    case {CalculateCallback(Args), ShouldCache} of
        {{ok, Value, _} = Res, true} ->
            bounded_cache:cache(Cache, Key, Value, Timestamp),
            Res;
        {Res, _} ->
            Res
    end.

-spec get_or_calculate_multiple_references(cache(), file_meta:uuid(), file_meta:doc(),
    callback(), get_or_calculate_options(), boolean()) -> get_or_calculate_return_value().
get_or_calculate_multiple_references(Cache, Key, #document{key = DocKey} = FileDoc, CalculateCallback, Options, ShouldCache) ->
    MergeCallback = maps:get(merge_callback, Options),
    References = get_references(FileDoc),
    ReferencesValues = lists:map(fun(#document{key = ReferenceKey} = ReferenceDoc) ->
        % NOTE: this function always calls CalculateCallback as there is high probability that value is not cached.
        % This is because cache invalidation always deletes all cached values and value is always calculated for one
        % or all references. As it has been tried to read value for reference passed in the argument before, it is
        % only possible that value is cached when get_or_calculate function is being executed by multiple processes
        % in parallel (and value has not been cached before). Thus, probability of finding value in cache is very
        % low so cache is not checked to avoid additional cost of call to bounded_cache.
        calculate_single_reference(Cache, ReferenceKey, ReferenceDoc, CalculateCallback, Options, false)
    end, References),

    case merge_references_values(ReferencesValues, undefined, MergeCallback) of
        {ok, MergedValue, MergeCalculationInfo} = OkAns ->
            {ok, CalculatedValue, CalculationInfo} = case maps:get(force_execution_on_referenced_key, Options, false) of
                true ->
                    INodeKey = fslogic_file_id:ensure_referenced_uuid(Key),
                    case lists:member(INodeKey, [DocKey | References]) of
                        true ->
                            OkAns;
                        false ->
                            force_execution_on_referenced_key(
                                INodeKey, CalculateCallback, MergeCallback, MergeCalculationInfo, MergedValue, Options)
                    end;
                false ->
                    OkAns
            end,

            differentiate_and_cache_references(Cache, Key, CalculatedValue, CalculationInfo,
                References, ReferencesValues, Options, ShouldCache);
        Error ->
            Error
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

-spec calculate_for_parent(cache(), file_meta:uuid(), file_meta:doc(), callback(),
    get_or_calculate_options()) -> get_or_calculate_return_value().
calculate_for_parent(Cache, Key, FileDoc, CalculateCallback, Options) ->
    case get_parent_uuid(Key, FileDoc) of
        {ok, ParentUuid} ->
            case get_file_meta(ParentUuid, Options) of
                {ok, ParentDoc} -> get_or_calculate(Cache, ParentDoc, CalculateCallback, Options);
                {error, not_found} ->
                    {error, {file_meta_missing, ParentUuid}}
            end;
        {error, _} = Error ->
            Error
    end.

-spec merge_references_values([get_or_calculate_return_value()], undefined | get_or_calculate_return_value(),
    merge_callback()) -> get_or_calculate_return_value().
merge_references_values([], Acc, _MergeCallback) ->
    Acc; % No references left - return answer
merge_references_values([Value | Tail], undefined, MergeCallback) ->
    merge_references_values(Tail, Value, MergeCallback); % First value - nothing to be merged
merge_references_values(_ReferenceValues, {error, _} = Acc, _MergeCallback) ->
    Acc; % Error occurred - return answer
merge_references_values([{error, _} = Error | _Tail], _Acc, _MergeCallback) ->
    Error; % Error occurred - return it
merge_references_values([{ok, Value, CalculationInfo} | Tail], {ok, AccValue, AccCalculationInfo}, MergeCallback) ->
    merge_references_values(Tail, MergeCallback(Value, AccValue, CalculationInfo, AccCalculationInfo), MergeCallback).

-spec differentiate_and_cache_references(cache(), file_meta:uuid(), bounded_cache:value(),
    calculation_info(), [file_meta:doc()], [get_or_calculate_return_value()], get_or_calculate_options(), boolean()) ->
    get_or_calculate_return_value().
differentiate_and_cache_references(Cache, _Key, MergedValue, CalculationInfo,
    References, ReferencesValues, #{timestamp := Timestamp, differentiate_callback := DifferentiateCallback}, ShouldCache) ->
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

    % Cache reference if all callback calls succeeded
    case FoldlAns of
        {ok, ReversedMappedReferencesValues} ->
            % Head of list is value calculated for reference for which get_or_calculate function has been called
            [ReturnValue | _] = MappedReferencesValues = lists:reverse(ReversedMappedReferencesValues),
            ShouldCache andalso lists:foreach(fun({#document{key = CacheKey}, ValueToCache}) ->
                bounded_cache:cache(Cache, CacheKey, ValueToCache, Timestamp)
            end, lists:zip(References, MappedReferencesValues)),    
            
            {ok, ReturnValue, CalculationInfo};
        FoldlError ->
            FoldlError
    end;
differentiate_and_cache_references(Cache, Key, MergedValue, CalculationInfo,
    _References, _ReferencesValues, #{timestamp := Timestamp}, ShouldCache) ->
    ShouldCache andalso bounded_cache:cache(Cache, Key, MergedValue, Timestamp),
    {ok, MergedValue, CalculationInfo}.

-spec force_execution_on_referenced_key(file_meta:uuid(), callback(), merge_callback(),
    calculation_info(), bounded_cache:value(), get_or_calculate_options()) -> get_or_calculate_return_value().
force_execution_on_referenced_key(INodeKey, CalculateCallback, MergeCallback, CalculationInfo, Acc, Options) ->
    case get_file_meta(INodeKey, Options) of
        {ok, FileDoc} ->
            Args = maps:get(args, Options, []),
            InitialCalculationInfo = maps:get(initial_calculation_info, Options, undefined),
            case CalculateCallback([FileDoc, undefined, InitialCalculationInfo | Args]) of
                {ok, Value, NewCalculationInfo} -> MergeCallback(Value, Acc, NewCalculationInfo, CalculationInfo);
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
-spec get_parent_uuid(file_meta:uuid(), file_meta:doc()) -> {ok, file_meta:uuid()} | {error, term()}.
get_parent_uuid(Key, #document{key = Key} = FileDoc) ->
    file_meta:get_parent_uuid(FileDoc);
get_parent_uuid(Key, _FileDoc) ->
    % Key differs from key inside FileDoc (see use_referenced_key option) - file doc for Key 
    % will be got inside get_parent_uuid function
    case file_meta:get_parent_uuid(Key) of
        {ok, ParentUuid} -> {ok, ParentUuid};
        {error, not_found} ->
            {error, {file_meta_missing, Key}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns #file_meta{} documents for all references.
%% NOTE: Head of list is always document passed by function's argument
%% (it is equal to document passed by get_or_calculate function argument).
%% @end
%%--------------------------------------------------------------------
-spec get_references(file_meta:doc()) -> [file_meta:doc()].
get_references(#document{key = DocKey} = FileDoc) ->
    %% @TODO VFS-7555 Use Doc for listing references after it is allowed
    {ok, References} = case fslogic_file_id:ensure_referenced_uuid(DocKey) of
        DocKey -> file_meta_hardlinks:list_references(FileDoc);
        ReferencedUuid -> file_meta_hardlinks:list_references(ReferencedUuid)
    end,

    [FileDoc | lists:filtermap(fun(Uuid) ->
        case file_meta:get(Uuid) of
            {ok, Doc} -> {true, Doc};
            {error, not_found} -> false
        end
    end, References -- [DocKey])].


-spec get_file_meta(file_meta:uuid(), get_or_calculate_options()) -> {ok, file_meta:doc()} | {error, term()}.
get_file_meta(Uuid, #{get_remote_from_scope := Scope}) ->
    file_meta:get_including_deleted_local_or_remote(Uuid, Scope);
get_file_meta(Uuid, _Options) ->
    file_meta:get_including_deleted(Uuid).