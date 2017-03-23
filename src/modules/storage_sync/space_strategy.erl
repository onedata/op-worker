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

-type job()             :: #space_strategy_job{}.
-type job_result()      :: term().
-type job_data()        :: term().

-type config()          :: {name(), arguments()}.

-type job_merge_type()  :: return_none | return_first | merge_all.
-type runnable()        :: {job_merge_type(), [job()]}.

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([definition/0, name/0, description/0, argument_name/0, argument_type/0, argument_value/0]).
-export_type([job/0, arguments/0, job_result/0, job_data/0, config/0, type/0]).
-export_type([job_merge_type/0, runnable/0]).

%% API
-export([]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%%===================================================================
%%% Internal functions
%%%===================================================================
