%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines `atm_store_container` interface - an object which can be
%%% used for storing and retrieving data of specific type for given store type.
%%%
%%%                             !!! Caution !!!
%%% 1) This behaviour must be implemented by modules with records of the same name.
%%% 2) Modules implementing this behaviour must also implement `persistent_record`
%%%    behaviour.
%%% 3) Modules implementing this behaviour must be registered in
%%%    `atm_store_type_to_atm_store_container_type` and
%%%    `atm_store_container_type_to_atm_store_type` functions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_container).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    create/2,
    copy/1,
    get_store_type/1, get_config/1, get_iterated_item_data_spec/1,
    acquire_iterator/1,
    browse_content/2,
    update_content/2,
    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type creation_args() :: #atm_store_container_creation_args{}.

-type type() ::
    atm_audit_log_store_container |
    atm_exception_store_container |
    atm_list_store_container |
    atm_range_store_container |
    atm_single_value_store_container |
    atm_time_series_store_container |
    atm_tree_forest_store_container.

-type initial_content() ::
    atm_audit_log_store_container:initial_content() |
    atm_exception_store_container:initial_content() |
    atm_list_store_container:initial_content() |
    atm_range_store_container:initial_content() |
    atm_single_value_store_container:initial_content() |
    atm_time_series_store_container:initial_content() |
    atm_tree_forest_store_container:initial_content().

-type record() ::
    atm_audit_log_store_container:record() |
    atm_exception_store_container:record() |
    atm_list_store_container:record() |
    atm_range_store_container:record() |
    atm_single_value_store_container:record() |
    atm_time_series_store_container:record() |
    atm_tree_forest_store_container:record().

-type content_browse_req() ::
    atm_audit_log_store_container:content_browse_req() |
    atm_exception_store_container:content_browse_req() |
    atm_list_store_container:content_browse_req() |
    atm_range_store_container:content_browse_req() |
    atm_single_value_store_container:content_browse_req() |
    atm_time_series_store_container:content_browse_req() |
    atm_tree_forest_store_container:content_browse_req().

-type content_update_req() ::
    atm_audit_log_store_container:content_update_req() |
    atm_exception_store_container:content_update_req() |
    atm_list_store_container:content_update_req() |
    atm_range_store_container:content_update_req() |
    atm_single_value_store_container:content_update_req() |
    atm_time_series_store_container:content_update_req() |
    atm_tree_forest_store_container:content_update_req().

-export_type([creation_args/0]).
-export_type([type/0, initial_content/0, record/0]).
-export_type([content_browse_req/0, content_update_req/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback create(creation_args()) -> record() | no_return().

-callback copy(record()) -> record() | no_return().

-callback get_config(record()) -> atm_store:config().

%%-------------------------------------------------------------------
%% @doc
%% Returns `atm_data_spec` for items returned by container iterator.
%% It may differ from `atm_data_spec` specified in atm_store_config as iteration
%% may return items inferred from store content rather than store content itself
%% (e.g. instead of returning `atm_range_value` it may returns integers from
%% specified range).
%% @end
%%-------------------------------------------------------------------
-callback get_iterated_item_data_spec(record()) -> atm_data_spec:record().

-callback acquire_iterator(record()) -> atm_store_container_iterator:record().

%%-------------------------------------------------------------------
%% @doc
%% Returns batch of exact items directly kept at store in opposition to
%% iteration which can return items inferred from store content.
%% @end
%%-------------------------------------------------------------------
-callback browse_content(record(), content_browse_req()) ->
    atm_store_content_browse_result:record() | no_return().

-callback update_content(record(), content_update_req()) ->
    ok | {ok, NewRecord :: record()} | no_return().

-callback delete(record()) -> ok | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_store:type(), creation_args()) -> record().
create(AtmStoreType, CreationArgs) ->
    RecordType = atm_store_type_to_atm_store_container_type(AtmStoreType),
    RecordType:create(CreationArgs).


-spec copy(record()) -> record() | no_return().
copy(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:copy(AtmStoreContainer).


-spec get_store_type(record()) -> atm_store:type().
get_store_type(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    atm_store_container_type_to_atm_store_type(RecordType).


-spec get_config(record()) -> atm_store:config().
get_config(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:get_config(AtmStoreContainer).


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:get_iterated_item_data_spec(AtmStoreContainer).


-spec acquire_iterator(record()) -> atm_store_container_iterator:record().
acquire_iterator(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:acquire_iterator(AtmStoreContainer).


-spec browse_content(record(), content_browse_req()) ->
    atm_store_content_browse_result:record() | no_return().
browse_content(AtmStoreContainer, AtmStoreContentBrowseReq) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:browse_content(AtmStoreContainer, AtmStoreContentBrowseReq).


-spec update_content(record(), content_update_req()) ->
    ok | {ok, record()} | no_return().
update_content(AtmStoreContainer, AtmStoreContentUpdateReq) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:update_content(AtmStoreContainer, AtmStoreContentUpdateReq).


-spec delete(record()) -> ok | no_return().
delete(AtmStoreContainer) ->
    RecordType = utils:record_type(AtmStoreContainer),
    RecordType:delete(AtmStoreContainer).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(AtmStoreContainer, NestedRecordEncoder) ->
    RecordType = utils:record_type(AtmStoreContainer),
    AtmStoreType = atm_store_container_type_to_atm_store_type(RecordType),

    maps:merge(
        #{<<"type">> => atm_store:type_to_json(AtmStoreType)},
        NestedRecordEncoder(AtmStoreContainer, RecordType)
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"type">> := AtmStoreTypeJson} = AtmStoreContainerJson, NestedRecordDecoder) ->
    AtmStoreType = atm_store:type_from_json(AtmStoreTypeJson),
    RecordType = atm_store_type_to_atm_store_container_type(AtmStoreType),

    NestedRecordDecoder(AtmStoreContainerJson, RecordType).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec atm_store_type_to_atm_store_container_type(atm_store:type()) ->
    atm_store_container:type().
atm_store_type_to_atm_store_container_type(audit_log) -> atm_audit_log_store_container;
atm_store_type_to_atm_store_container_type(exception) -> atm_exception_store_container;
atm_store_type_to_atm_store_container_type(list) -> atm_list_store_container;
atm_store_type_to_atm_store_container_type(range) -> atm_range_store_container;
atm_store_type_to_atm_store_container_type(single_value) -> atm_single_value_store_container;
atm_store_type_to_atm_store_container_type(time_series) -> atm_time_series_store_container;
atm_store_type_to_atm_store_container_type(tree_forest) -> atm_tree_forest_store_container.


%% @private
-spec atm_store_container_type_to_atm_store_type(atm_store_container:type()) ->
    atm_store:type().
atm_store_container_type_to_atm_store_type(atm_audit_log_store_container) -> audit_log;
atm_store_container_type_to_atm_store_type(atm_exception_store_container) -> exception;
atm_store_container_type_to_atm_store_type(atm_list_store_container) -> list;
atm_store_container_type_to_atm_store_type(atm_range_store_container) -> range;
atm_store_container_type_to_atm_store_type(atm_single_value_store_container) -> single_value;
atm_store_container_type_to_atm_store_type(atm_time_series_store_container) -> time_series;
atm_store_container_type_to_atm_store_type(atm_tree_forest_store_container) -> tree_forest.
