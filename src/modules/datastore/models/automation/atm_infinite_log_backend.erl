%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with the persistent list store backend implemented using infinite log.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_infinite_log_backend).
-author("Michal Stanisz").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, append/3, destroy/1, list/4]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore_infinite_log:key().

-export_type([id/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================

-spec create(infinite_log:log_opts()) -> {ok, id()}.
create(Opts) ->
    Id = datastore_key:new(),
    ok = datastore_infinite_log:create(?CTX, Id, Opts),
    {ok, Id}.


-spec append(id(), infinite_log:content(), atm_data_spec:record()) -> ok.
append(Id, Item, AtmDataSpec) ->
    CompressedItem = atm_value:compress(Item, AtmDataSpec),
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(CompressedItem)).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec list(atm_workflow_execution_ctx:record(), id(), infinite_log_browser:listing_opts(), 
    atm_data_spec:record()) -> {ok, infinite_log_browser:listing_result()} | {error, term()}.
list(AtmWorkflowExecutionCtx, Id, Opts, AtmDataSpec) ->
    case datastore_infinite_log:list(?CTX, Id, Opts) of
        {ok, {Marker, Entries}} ->
            {ok, {Marker, expand_entries(AtmWorkflowExecutionCtx, Entries, AtmDataSpec)}};
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec expand_entries(atm_workflow_execution_ctx:record(), infinite_log_browser:entry_series(), 
    atm_data_spec:record()) -> infinite_log_browser:entry_series().
expand_entries(AtmWorkflowExecutionCtx, Entries, AtmDataSpec) ->
    lists:map(fun({EntryIndex, {Timestamp, Value}}) ->
        CompressedValue = json_utils:decode(Value),
        Item = case atm_value:expand(AtmWorkflowExecutionCtx, CompressedValue, AtmDataSpec) of
            {ok, Res} -> 
                {ok, #{
                    <<"timestamp">> => Timestamp, 
                    <<"entry">> => Res
                }};
            {error, _} -> 
                ?ERROR_FORBIDDEN
        end,
        {integer_to_binary(EntryIndex), Item}
    end, Entries).