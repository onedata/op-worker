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
-export([version/0, db_encode/2, db_decode/2, upgrade_encoded_record/2]).


-type initial_value() :: [automation:item()] | undefined.
%% Full 'operation_options' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying individual binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"isBatch">> := boolean()
%% }
%@formatter:off
-type operation_options() :: #{binary() => boolean()}.
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    offset => atm_store_api:offset()
}.
%@formatter:on

-record(atm_list_store_container, {
    data_spec :: atm_data_spec:record(),
    backend_id :: json_infinite_log_model:id()
}).
-type record() :: #atm_list_store_container{}.

-export_type([initial_value/0, operation_options/0, browse_options/0, record/0]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_auth:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(_AtmWorkflowExecutionAuth, AtmDataSpec, undefined) ->
    create_container(AtmDataSpec);

create(AtmWorkflowExecutionAuth, AtmDataSpec, InitialValueBatch) ->
    validate_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, InitialValueBatch),
    append_insecure(InitialValueBatch, create_container(AtmDataSpec)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_list_store_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec browse_content(atm_workflow_execution_auth:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_list_store_container{
    backend_id = BackendId,
    data_spec = AtmDataSpec
}) ->
    atm_infinite_log_based_stores_common:browse_content(
        list_store, BackendId, BrowseOpts,
        atm_list_store_container_iterator:gen_listing_postprocessor(AtmWorkflowExecutionAuth, AtmDataSpec)
    ).


-spec acquire_iterator(record()) -> atm_list_store_container_iterator:record().
acquire_iterator(#atm_list_store_container{backend_id = BackendId}) ->
    atm_list_store_container_iterator:build(BackendId).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(#atm_list_store_container{data_spec = AtmDataSpec} = Record, #atm_store_container_operation{
    type = append,
    options = #{<<"isBatch">> := true},
    argument = Batch,
    workflow_execution_auth = AtmWorkflowExecutionAuth
}) ->
    validate_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, Batch),
    append_insecure(Batch, Record);

apply_operation(#atm_list_store_container{} = Record, Operation = #atm_store_container_operation{
    type = append,
    argument = Item,
    options = Options
}) ->
    apply_operation(Record, Operation#atm_store_container_operation{
        options = Options#{<<"isBatch">> => true},
        argument = [Item]
    });

apply_operation(_Record, _Operation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_list_store_container{backend_id = BackendId}) ->
    json_infinite_log_model:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    2.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_list_store_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}, NestedRecordEncoder) ->
    #{
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"backendId">> => BackendId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson, <<"backendId">> := BackendId}, NestedRecordDecoder) ->
    #atm_list_store_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        backend_id = BackendId
    }.


-spec upgrade_encoded_record(persistent_record:record_version(), json_utils:json_term()) ->
    {persistent_record:record_version(), json_utils:json_term()}.
upgrade_encoded_record(1, #{<<"atmInfiniteLogContainer">> := AtmInfiniteLogContainerIteratorJson}) ->
    #{<<"_data">> := #{
        <<"backendId">> := BackendId,
        <<"dataSpec">> := AtmDataSpec
    }} = AtmInfiniteLogContainerIteratorJson,
    {2, #{
        <<"backendId">> => BackendId,
        <<"dataSpec">> => AtmDataSpec
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_container(atm_data_spec:record()) -> record().
create_container(AtmDataSpec) ->
    {ok, Id} = json_infinite_log_model:create(#{}),
    #atm_list_store_container{
        data_spec = AtmDataSpec,
        backend_id = Id
    }.


%% @private
-spec validate_data_batch(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    [json_utils:json_term()]
) ->
    ok | no_return().
validate_data_batch(AtmWorkflowExecutionAuth, AtmDataSpec, Batch) when is_list(Batch) ->
    lists:foreach(fun(Item) ->
        atm_value:validate(AtmWorkflowExecutionAuth, Item, AtmDataSpec)
    end, Batch);
validate_data_batch(_AtmWorkflowExecutionAuth, _AtmDataSpec, _Item) ->
    throw(?ERROR_BAD_DATA(<<"value">>, <<"not a batch">>)).


%% @private
-spec append_insecure([automation:item()], record()) -> record().
append_insecure(Batch, Record = #atm_list_store_container{
    data_spec = AtmDataSpec,
    backend_id = BackendId
}) ->
    lists:foreach(fun(Item) ->
        ok = json_infinite_log_model:append(
            BackendId, atm_value:compress(Item, AtmDataSpec))
    end, Batch),
    Record.
