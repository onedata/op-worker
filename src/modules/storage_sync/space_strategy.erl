%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for handling space strategies. Defines base data types.
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategy).
-author("Rafal Slota").

-include("modules/storage_sync/strategy_config.hrl").
-include("global_definitions.hrl").


%%%===================================================================
%%% Types
%%%===================================================================

-type type()            :: storage_update | storage_import | filename_mapping|
                           file_caching | enoent_handling | file_conflict_resolution.
-type definition()      :: #space_strategy{}.
-type name()            :: atom().
-type arguments()       :: maps:map(). %todo dialyzer crashes on: #{argument_name() => argument_type()}.
-type argument_name()   :: atom().
-type argument_type()   :: integer   | float   | string   | boolean.
-type argument_value()  :: integer() | float() | binary() | boolean().
-type description()     :: binary().
-type timestamp()       :: non_neg_integer() | undefined.

-type job()             :: #space_strategy_job{}.
-type job_result()      :: term().
-type job_data()        :: maps:map().

-type config()          :: {name(), arguments()}.

-type job_merge_type()  :: return_none | return_first | merge_all.
-type runnable()        :: {job_merge_type(), [job()]}.

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([definition/0, name/0, description/0, argument_name/0,
    argument_type/0, argument_value/0, timestamp/0]).
-export_type([job/0, arguments/0, job_result/0, job_data/0, config/0, type/0]).
-export_type([job_merge_type/0, runnable/0]).

%% API
-export([types/0, default_worker_pool_config/0, default_main_worker_pool/0,
    update_job_data/3, take_from_job_data/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns list of space_strategy types.
%% @end
%%-------------------------------------------------------------------
-spec types() -> [type()].
types() -> [
    storage_update, storage_import, filename_mapping,
    file_caching, enoent_handling, file_conflict_resolution
].

%%-------------------------------------------------------------------
%% @doc
%% Default config of worker_pool for space_strategies.
%% @end
%%-------------------------------------------------------------------
-spec default_worker_pool_config() -> [{worker_pool:name(), non_neg_integer()}].
default_worker_pool_config() ->
    {ok, WorkersNum} = application:get_env(?APP_NAME, ?GENERIC_STRATEGY_WORKERS_NUM_KEY),
    [{default_main_worker_pool(), WorkersNum}].

%%-------------------------------------------------------------------
%% @doc
%% Default pool for space_strategies.
%% @end
%%-------------------------------------------------------------------
-spec default_main_worker_pool() -> worker_pool:name().
default_main_worker_pool() ->
    ?GENERIC_STRATEGY_POOL_NAME.

%%-------------------------------------------------------------------
%% @doc
%% Updates space_strategy_job data map with new Key, Value pair.
%% @end
%%-------------------------------------------------------------------
-spec update_job_data(atom(), term(), job()) -> job().
update_job_data(Key, Value, Job = #space_strategy_job{data = Data}) ->
    Job#space_strategy_job{data = Data#{Key => Value}}.

%%-------------------------------------------------------------------
%% @doc
%% Takes (gets and removes) value from space_strategy_job data map.
%% @end
%%-------------------------------------------------------------------
-spec take_from_job_data(atom(), job(), term()) -> {term(), job()}.
take_from_job_data(Key, Job = #space_strategy_job{data = Data}, Default) ->
    case maps:take(Key, Data) of
        {Value, Data2} ->
            {Value, Job#space_strategy_job{data = Data2}};
        error ->
            {Default, Job}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
