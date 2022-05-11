%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `time_series`
%%% atm_store type.
%%%
%%%                             !!! Caution !!!
%%% This store does not support iteration and should never be referenced by
%%% schema in such context.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_store_container).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_browser.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_store_container callbacks
-export([
    create/3,
    get_config/1,

    get_iterated_item_data_spec/1,
    acquire_iterator/1,

    browse_content/2,
    update_content/2,

    delete/1
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).

%% datastore model callbacks
-export([get_ctx/0]).


-type initial_content() :: undefined.

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_time_series_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_time_series_store_content_update_options:record()
}.

-record(atm_time_series_store_container, {
    config :: atm_time_series_store_config:record(),
    backend_id :: time_series_collection:id()
}).
-type record() :: #atm_time_series_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


-define(ANY_MEASUREMENT_DATA_SPEC, #atm_data_spec{
    type = atm_time_series_measurement_type,
    value_constraints = #{specs => [#atm_time_series_measurement_spec{
        name_matcher_type = has_prefix,
        name_matcher = <<>>,
        unit = none
    }]}
}).


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    atm_time_series_store_config:record(),
    initial_content()
) ->
    record() | no_return().
create(_AtmWorkflowExecutionAuth, AtmStoreConfig, undefined) ->
    BackendId = datastore_key:new(),
    ok = datastore_time_series_collection:create(?CTX, BackendId, build_initial_ts_collection_config(
        AtmStoreConfig#atm_time_series_store_config.schemas
    )),

    #atm_time_series_store_container{
        config = AtmStoreConfig,
        backend_id = BackendId
    };

create(_AtmWorkflowExecutionAuth, _AtmStoreConfig, _InitialContent) ->
    throw(?ERROR_BAD_DATA(
        <<"initialContent">>,
        <<"Time series store does not accept initial content">>
    )).


-spec get_config(record()) -> atm_time_series_store_config:record().
get_config(#atm_time_series_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> no_return().
get_iterated_item_data_spec(#atm_time_series_store_container{}) ->
    error(not_supported).


-spec acquire_iterator(record()) -> no_return().
acquire_iterator(#atm_time_series_store_container{}) ->
    error(not_supported).


-spec browse_content(record(), content_browse_req()) ->
    atm_time_series_store_content_browse_result:record() | no_return().
browse_content(Record, #atm_store_content_browse_req{
    options = #atm_time_series_store_content_browse_options{
        request = #time_series_get_layout_request{}
    }
}) ->
    {ok, LayoutResult} = datastore_time_series_collection:browse(
        ?CTX, Record#atm_time_series_store_container.backend_id, #time_series_get_layout_request{}
    ),
    #atm_time_series_store_content_browse_result{
        result = LayoutResult
    };

browse_content(Record, #atm_store_content_browse_req{
    options = #atm_time_series_store_content_browse_options{
        request = #time_series_get_slice_request{
            layout = SliceLayout,
            start_timestamp = StartTimestamp,
            window_limit = WindowLimit
        }
    }
}) ->
    case datastore_time_series_collection:browse(
        ?CTX,
        Record#atm_time_series_store_container.backend_id,
        #time_series_get_slice_request{
            layout = SliceLayout,
            start_timestamp = StartTimestamp,
            window_limit = WindowLimit
        }
    ) of
        {ok, SliceResult} ->
            #atm_time_series_store_content_browse_result{result = SliceResult};
        ?ERROR_NOT_FOUND ->
            throw(?ERROR_NOT_FOUND);
        ?ERROR_TSC_MISSING_LAYOUT(MissingLayout) ->
            throw(?ERROR_TSC_MISSING_LAYOUT(MissingLayout))
    end.


-spec update_content(record(), content_update_req()) -> record() | no_return().
update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Measurements,
    options = #atm_time_series_store_content_update_options{dispatch_rules = DispatchRules}
}) when is_list(Measurements) ->
    atm_value:validate(
        AtmWorkflowExecutionAuth,
        Measurements,
        ?ATM_ARRAY_DATA_SPEC(?ANY_MEASUREMENT_DATA_SPEC)
    ),
    consume_measurements(Measurements, DispatchRules, Record);

update_content(Record, #atm_store_content_update_req{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    argument = Measurement,
    options = #atm_time_series_store_content_update_options{dispatch_rules = DispatchRules}
}) ->
    atm_value:validate(AtmWorkflowExecutionAuth, Measurement, ?ANY_MEASUREMENT_DATA_SPEC),
    consume_measurements([Measurement], DispatchRules, Record).


-spec delete(record()) -> ok.
delete(#atm_time_series_store_container{backend_id = BackendId}) ->
    ok = datastore_time_series_collection:delete(?CTX, BackendId).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_time_series_store_container{
    config = AtmStoreConfig,
    backend_id = BackendId
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_time_series_store_config),
        <<"backendId">> => BackendId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"config">> := AtmStoreConfigJson,
    <<"backendId">> := BackendId
}, NestedRecordDecoder) ->
    #atm_time_series_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_time_series_store_config),
        backend_id = BackendId
    }.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns store initial time series collection config. It is created using only
%% 'exact' generator type schemes. Other time series are created on demand when
%% consuming measurements (it is not possible to foresee what time series will
%% be created for other, dynamic generators).
%% @end
%%-------------------------------------------------------------------
-spec build_initial_ts_collection_config([atm_time_series_schema:record()]) ->
    time_series_collection:config().
build_initial_ts_collection_config(TSSchemas) ->
    lists:foldl(fun
        (TSSchema = #atm_time_series_schema{name_generator_type = exact}, Acc) ->
            TSName = TSSchema#atm_time_series_schema.name_generator,
            TSConfig = TSSchema#atm_time_series_schema.metrics,
            Acc#{TSName => TSConfig};
        (_, Acc) ->
            Acc
    end, #{}, TSSchemas).


%% @private
-spec consume_measurements(
    [json_utils:json_map()],
    [atm_time_series_dispatch_rule:record()],
    record()
) ->
    record().
consume_measurements(Measurements, DispatchRules, Record = #atm_time_series_store_container{
    config = #atm_time_series_store_config{schemas = TSSchemas},
    backend_id = BackendId
}) ->
    {ConsumeSpec, InvolvedCollectionConfig} = lists:foldl(fun(Measurement, {ConsumeSpecOuterAcc, InvolvedConfigOuterAcc}) ->
        #{<<"tsName">> := MeasurementTSName, <<"timestamp">> := Timestamp, <<"value">> := Value} = Measurement,
        MatchingDispatchRules = atm_time_series_names:find_matching_dispatch_rules(MeasurementTSName, DispatchRules),

        lists:foldl(fun(DispatchRule, {ConsumeSpecInnerAcc, InvolvedConfigInnerAcc}) ->
            TSSchema = atm_time_series_names:select_referenced_time_series_schema(DispatchRule, TSSchemas),
            TargetTSName = atm_time_series_names:resolve_target_ts_name(MeasurementTSName, TSSchema, DispatchRule),
            UpdatedConsumeSpec = kv_utils:update_with([TargetTSName, ?ALL_METRICS], fun(PreviousMeasurements) ->
                [{Timestamp, Value} | PreviousMeasurements]
            end, [{Timestamp, Value}], ConsumeSpecInnerAcc),
            {UpdatedConsumeSpec, InvolvedConfigInnerAcc#{TargetTSName => TSSchema#atm_time_series_schema.metrics}}

        end, {ConsumeSpecOuterAcc, InvolvedConfigOuterAcc}, MatchingDispatchRules)
    end, {#{}, #{}}, Measurements),

    case datastore_time_series_collection:consume_measurements(?CTX, BackendId, ConsumeSpec) of
        ok ->
            ok;
        ?ERROR_TSC_MISSING_LAYOUT(MissingLayout) ->
            MissingConfig = maps:with(maps:keys(MissingLayout), InvolvedCollectionConfig),
            ok = datastore_time_series_collection:incorporate_config(?CTX, BackendId, MissingConfig),
            ok = datastore_time_series_collection:consume_measurements(?CTX, BackendId, ConsumeSpec)
    end,

    Record.
