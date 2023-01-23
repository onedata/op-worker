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
%%% which in turn depends on that `apply_operation` doesn't change container.
%%% Any changes made in this module may affect logger and should be
%%% accounted for.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_container).
-author("Lukasz Opiola").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/audit_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    copy/1,
    get_config/1,

    get_iterated_item_data_spec/1,
    acquire_iterator/1,

    browse_content/2,
    update_content/2,

    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_content() :: [atm_value:expanded()] | undefined.

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_audit_log_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_audit_log_store_content_update_options:record()
}.

-record(atm_audit_log_store_container, {
    config :: atm_audit_log_store_config:record(),
    backend_id :: audit_log:id()
}).
-type record() :: #atm_audit_log_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


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
    Logs = sanitize_append_requests(AtmWorkflowExecutionAuth, LogContentDataSpec, InitialItemsArray),

    extend_audit_log(Logs, create_container(AtmStoreConfig)).


-spec copy(record()) -> no_return().
copy(_) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec get_config(record()) -> atm_audit_log_store_config:record().
get_config(#atm_audit_log_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(_) ->
    #atm_data_spec{type = atm_object_type}.


-spec acquire_iterator(record()) -> atm_audit_log_store_container_iterator:record().
acquire_iterator(#atm_audit_log_store_container{backend_id = BackendId}) ->
    atm_audit_log_store_container_iterator:build(BackendId).


-spec browse_content(record(), content_browse_req()) ->
    atm_audit_log_store_content_browse_result:record() | no_return().
browse_content(Record, #atm_store_content_browse_req{
    options = #atm_audit_log_store_content_browse_options{browse_opts = BrowseOpts}
}) ->
    {ok, BrowseResult} = audit_log:browse(
        Record#atm_audit_log_store_container.backend_id,
        BrowseOpts
    ),
    #atm_audit_log_store_content_browse_result{result = BrowseResult}.


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = ItemsArray,
    options = #atm_audit_log_store_content_update_options{function = extend}
}) ->
    AppendRequests = sanitize_append_requests(
        AtmWorkflowExecutionAuth,
        get_log_content_data_spec(Record),
        ItemsArray
    ),
    extend_audit_log(AppendRequests, Record);

update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Item,
    options = #atm_audit_log_store_content_update_options{function = append}
}) ->
    AppendRequest = sanitize_append_request(
        AtmWorkflowExecutionAuth,
        get_log_content_data_spec(Record),
        Item
    ),
    append_to_audit_log(AppendRequest, Record).


-spec delete(record()) -> ok.
delete(#atm_audit_log_store_container{backend_id = BackendId}) ->
    ok = audit_log:delete(BackendId).


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
    {ok, Id} = audit_log:create(#{}),

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
-spec sanitize_append_requests(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    [json_utils:json_term() | audit_log:append_request()]
) ->
    [audit_log:append_request()] | no_return().
sanitize_append_requests(AtmWorkflowExecutionAuth, LogContentDataSpec, ItemsArray) when is_list(ItemsArray) ->
    Requests = lists:map(fun build_audit_log_append_request/1, ItemsArray),

    atm_value:validate(
        AtmWorkflowExecutionAuth,
        lists:map(fun(#audit_log_append_request{content = LogContent}) -> LogContent end, Requests),
        ?ATM_ARRAY_DATA_SPEC(LogContentDataSpec)
    ),

    Requests;

sanitize_append_requests(_AtmWorkflowExecutionAuth, _LogContentDataSpec, Item) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Item, atm_array_type)).


%% @private
-spec sanitize_append_request(
    atm_workflow_execution_auth:record(),
    atm_data_spec:record(),
    json_utils:json_term() | audit_log:append_request()
) ->
    audit_log:append_request() | no_return().
sanitize_append_request(AtmWorkflowExecutionAuth, LogContentDataSpec, Item) ->
    Request = #audit_log_append_request{content = LogContent} = build_audit_log_append_request(Item),
    atm_value:validate(AtmWorkflowExecutionAuth, LogContent, LogContentDataSpec),

    Request.


%% @private
-spec build_audit_log_append_request(json_utils:json_term() | audit_log:append_request()) ->
    audit_log:append_request().
build_audit_log_append_request(#audit_log_append_request{} = AppendRequest) ->
    AppendRequest;

build_audit_log_append_request(#{<<"content">> := LogContent, <<"severity">> := Severity}) ->
    #audit_log_append_request{
        severity = audit_log:normalize_severity(Severity),
        source = ?USER_AUDIT_LOG_ENTRY_SOURCE,
        content = LogContent
    };

build_audit_log_append_request(#{<<"content">> := LogContent}) ->
    #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        source = ?USER_AUDIT_LOG_ENTRY_SOURCE,
        content = LogContent
    };

build_audit_log_append_request(#{<<"severity">> := Severity} = Object) ->
    #audit_log_append_request{
        severity = audit_log:normalize_severity(Severity),
        source = ?USER_AUDIT_LOG_ENTRY_SOURCE,
        content = maps:without([<<"severity">>], Object)
    };

build_audit_log_append_request(LogContent) ->
    #audit_log_append_request{
        severity = ?INFO_AUDIT_LOG_SEVERITY,
        source = ?USER_AUDIT_LOG_ENTRY_SOURCE,
        content = LogContent
    }.


%% @private
-spec extend_audit_log([audit_log:append_request()], record()) -> record().
extend_audit_log(AppendRequests, Record) ->
    lists:foldl(fun append_to_audit_log/2, Record, AppendRequests).


%% @private
-spec append_to_audit_log(audit_log:append_request(), record()) -> record().
append_to_audit_log(AppendRequest, Record = #atm_audit_log_store_container{
    backend_id = BackendId
}) ->
    ok = audit_log:append(BackendId, AppendRequest),
    Record.
