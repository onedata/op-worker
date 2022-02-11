%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `audit_log`
%%% atm_store type.
%%%
%%%                             !! CAUTION !!
%%% This store container is directly used by `atm_workflow_execution_logger`
%%% which in turn depends that `apply_operation` doesn't change container.
%%% Any changes made in this module may affect logger and should be
%%% accounted for.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_container).
-author("Lukasz Opiola").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    get_config/1, get_iterated_item_data_spec/1,
    browse_content/3, acquire_iterator/1,
    apply_operation/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_content() :: [atm_value:expanded()] | undefined.
-type operation_options() :: #{}.  %% for now no options are supported

%@formatter:off
-type browse_options() :: #{
    limit := atm_store_api:limit(),
    start_index => atm_store_api:index(),
    start_timestamp => time:millis(),
    offset => atm_store_api:offset()
}.
%@formatter:on

-record(atm_audit_log_store_container, {
    config :: atm_audit_log_store_config:record(),
    backend_id :: json_infinite_log_model:id()
}).
-type record() :: #atm_audit_log_store_container{}.

-export_type([initial_content/0, operation_options/0, browse_options/0, record/0]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    atm_audit_log_store_config:record(),
    initial_content()
) ->
    record() | no_return().
create(_AtmWorkflowExecutionAuth, AtmStoreConfig, undefined) ->
    create_container(AtmStoreConfig);

create(AtmWorkflowExecutionAuth, AtmStoreConfig, InitialItemsArray) ->
    % validate and sanitize given array of items first, to simulate atomic operation
    LogContentDataSpec = AtmStoreConfig#atm_audit_log_store_config.log_content_data_spec,
    Logs = sanitize_items_array(AtmWorkflowExecutionAuth, LogContentDataSpec, InitialItemsArray),

    extend_with_sanitized_items_array(Logs, create_container(AtmStoreConfig)).


-spec get_config(record()) -> atm_audit_log_store_config:record().
get_config(#atm_audit_log_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(_) ->
    #atm_data_spec{type = atm_object_type}.


-spec browse_content(atm_workflow_execution_auth:record(), browse_options(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(AtmWorkflowExecutionAuth, BrowseOpts, #atm_audit_log_store_container{
    config = #atm_audit_log_store_config{log_content_data_spec = LogContentDataSpec},
    backend_id = BackendId
}) ->
    atm_infinite_log_based_stores_common:browse_content(
        audit_log_store, BackendId, BrowseOpts,
        atm_audit_log_store_container_iterator:gen_listing_postprocessor(
            AtmWorkflowExecutionAuth, LogContentDataSpec
        )
    ).


-spec acquire_iterator(record()) -> atm_audit_log_store_container_iterator:record().
acquire_iterator(#atm_audit_log_store_container{
    config = #atm_audit_log_store_config{log_content_data_spec = LogContentDataSpec},
    backend_id = BackendId
}) ->
    atm_audit_log_store_container_iterator:build(LogContentDataSpec, BackendId).


-spec apply_operation(record(), atm_store_container:operation()) ->
    record() | no_return().
apply_operation(Record, #atm_store_container_operation{
    type = extend,
    argument = ItemsArray,
    workflow_execution_auth = AtmWorkflowExecutionAuth
}) ->
    % validate and sanitize given array of items first, to simulate atomic operation
    LogContentDataSpec = get_log_content_data_spec(Record),
    Logs = sanitize_items_array(AtmWorkflowExecutionAuth, LogContentDataSpec, ItemsArray),
    extend_with_sanitized_items_array(Logs, Record);

apply_operation(Record, #atm_store_container_operation{
    type = append,
    argument = Item,
    workflow_execution_auth = AtmWorkflowExecutionAuth
}) ->
    % validate and sanitize given item, to simulate atomic operation
    Log = sanitize_item(AtmWorkflowExecutionAuth, get_log_content_data_spec(Record), Item),
    append_sanitized_item(Log, Record);

apply_operation(_Record, _Operation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(#atm_audit_log_store_container{backend_id = BackendId}) ->
    json_infinite_log_model:destroy(BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_audit_log_store_container{
    config = AtmStoreConfig,
    backend_id = BackendId
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_audit_log_store_config),
        <<"backendId">> => BackendId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(
    #{<<"config">> := AtmStoreConfigJson, <<"backendId">> := BackendId},
    NestedRecordDecoder
) ->
    #atm_audit_log_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_audit_log_store_config),
        backend_id = BackendId
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_container(atm_audit_log_store_config:record()) -> record().
create_container(AtmStoreConfig) ->
    {ok, Id} = json_infinite_log_model:create(#{}),
    #atm_audit_log_store_container{
        config = AtmStoreConfig,
        backend_id = Id
    }.


%% @private
-spec get_log_content_data_spec(record()) -> atm_data_spec:record().
get_log_content_data_spec(#atm_audit_log_store_container{
    config = #atm_audit_log_store_config{log_content_data_spec = LogContentDataSpec}
}) ->
    LogContentDataSpec.


%% @private
-spec sanitize_items_array(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    [json_utils:json_term()]
) ->
    [atm_value:expanded()] | no_return().
sanitize_items_array(AtmWorkflowExecutionAuth, LogContentDataSpec, ItemsArray) when is_list(ItemsArray) ->
    Logs = lists:map(fun prepare_audit_log_object/1, ItemsArray),

    LogContents = lists:map(fun(#{<<"content">> := LogContent}) -> LogContent end, Logs),
    atm_value:validate(AtmWorkflowExecutionAuth, LogContents, ?ATM_ARRAY_DATA_SPEC(LogContentDataSpec)),

    Logs;
sanitize_items_array(_AtmWorkflowExecutionAuth, _LogContentDataSpec, Item) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Item, atm_array_type)).


%% @private
-spec sanitize_item(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    json_utils:json_term()
) ->
    atm_value:expanded() | no_return().
sanitize_item(AtmWorkflowExecutionAuth, LogContentDataSpec, Item) ->
    Log = #{<<"content">> := LogContent} = prepare_audit_log_object(Item),
    atm_value:validate(AtmWorkflowExecutionAuth, LogContent, LogContentDataSpec),
    Log.


%% @private
-spec extend_with_sanitized_items_array([atm_value:expanded()], record()) -> record().
extend_with_sanitized_items_array(LogsArray, Record) ->
    lists:foldl(fun append_sanitized_item/2, Record, LogsArray).


%% @private
-spec append_sanitized_item(atm_value:expanded(), record()) -> record().
append_sanitized_item(Log, Record = #atm_audit_log_store_container{
    config = #atm_audit_log_store_config{log_content_data_spec = LogContentDataSpec},
    backend_id = BackendId
}) ->
    {LogContent, LogWithoutContent} = maps:take(<<"content">>, Log),
    ok = json_infinite_log_model:append(BackendId, LogWithoutContent#{
        <<"compressedContent">> => atm_value:compress(LogContent, LogContentDataSpec)
    }),
    Record.


%% @private
-spec prepare_audit_log_object(atm_value:expanded()) -> json_utils:json_map().
prepare_audit_log_object(#{<<"content">> := LogContent, <<"severity">> := Severity}) ->
    #{
        <<"content">> => LogContent,
        <<"severity">> => normalize_severity(Severity)
    };
prepare_audit_log_object(#{<<"content">> := LogContent}) ->
    #{
        <<"content">> => LogContent,
        <<"severity">> => ?LOGGER_INFO
    };
prepare_audit_log_object(#{<<"severity">> := Severity} = Object) ->
    #{
        <<"content">> => maps:without([<<"severity">>], Object),
        <<"severity">> => normalize_severity(Severity)
    };
prepare_audit_log_object(LogContent) ->
    #{
        <<"content">> => LogContent,
        <<"severity">> => ?LOGGER_INFO
    }.


%% @private
-spec normalize_severity(any()) -> binary().
normalize_severity(ProvidedSeverity) ->
    case lists:member(ProvidedSeverity, ?LOGGER_SEVERITIES) of
        true -> ProvidedSeverity;
        false -> ?LOGGER_INFO
    end.
