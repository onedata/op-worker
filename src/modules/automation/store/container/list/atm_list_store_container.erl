%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `list`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_list_store_container).
-author("Michal Stanisz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    get_data_spec/1, browse_content/3, acquire_iterator/1,
    apply_operation/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_value() :: atm_infinite_log_container:initial_value().
-type operation_options() :: atm_infinite_log_container:operation_options().
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    offset => atm_store_api:offset()
}.

-record(atm_list_store_container, {
%%    @TODO VFS-8068 Do not use atm_infinite_log_container
    atm_infinite_log_container :: atm_infinite_log_container:record()
}).
-type record() :: #atm_list_store_container{}.

-export_type([initial_value/0, operation_options/0, browse_options/0, record/0]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_ctx:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch) ->
    #atm_list_store_container{
        atm_infinite_log_container = atm_infinite_log_container:create(
            AtmWorkflowExecutionCtx, AtmDataSpec, InitialValueBatch
        )
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_list_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    atm_infinite_log_container:get_data_spec(AtmInfiniteLogContainer).


-spec browse_content(atm_workflow_execution_ctx:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionCtx, BrowseOpts, #atm_list_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}) ->
    SanitizedBrowseOpts = sanitize_browse_options(BrowseOpts),
    {Entries, IsLast} = atm_infinite_log_container:browse_content(
        SanitizedBrowseOpts, AtmInfiniteLogContainer),
    AtmDataSpec = atm_infinite_log_container:get_data_spec(AtmInfiniteLogContainer),
    MappedEntries = lists:map(fun(ListedEntry) ->
        {Index, Compressed, _Timestamp} = 
            atm_infinite_log_backend:extract_listed_entry(ListedEntry),
        {Index, atm_value:expand(AtmWorkflowExecutionCtx, Compressed, AtmDataSpec)}
    end, Entries),
    {MappedEntries, IsLast}.


-spec acquire_iterator(record()) -> atm_list_store_container_iterator:record().
acquire_iterator(#atm_list_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    AtmInfiniteLogContainerIterator = atm_infinite_log_container:acquire_iterator(AtmInfiniteLogContainer),
    atm_list_store_container_iterator:build(AtmInfiniteLogContainerIterator).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(AtmListStoreContainer = #atm_list_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}, AtmStoreContainerOperation) ->
    AtmListStoreContainer#atm_list_store_container{
        atm_infinite_log_container = atm_infinite_log_container:apply_operation(
            AtmInfiniteLogContainer, AtmStoreContainerOperation
        )
    }.


-spec delete(record()) -> ok.
delete(#atm_list_store_container{atm_infinite_log_container = AtmInfiniteLogContainer}) ->
    atm_infinite_log_container:delete(AtmInfiniteLogContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_store_container{
    atm_infinite_log_container = AtmInfiniteLogContainer
}, NestedRecordEncoder) ->
        #{
            <<"atmInfiniteLogContainer">> => NestedRecordEncoder(
                AtmInfiniteLogContainer, atm_infinite_log_container
            )
        }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"atmInfiniteLogContainer">> := AtmInfiniteLogContainerJson}, NestedRecordDecoder) ->
    #atm_list_store_container{
        atm_infinite_log_container = NestedRecordDecoder(
            AtmInfiniteLogContainerJson, atm_infinite_log_container
        )
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec sanitize_browse_options(browse_options()) -> browse_options().
sanitize_browse_options(BrowseOpts) ->
    maps:without([start_timestamp], middleware_sanitizer:sanitize_data(BrowseOpts, #{
        required => #{
            limit => {integer, {not_lower_than, 1}}
        },
        at_least_one => #{
            offset => {integer, any},
            start_index => {binary, any}
        }
    })).
