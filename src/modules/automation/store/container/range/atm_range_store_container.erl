%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_store_container` functionality for `range`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_container).
-author("Bartosz Walkowicz").

-behaviour(atm_store_container).
-behaviour(persistent_record).

-include("modules/automation/atm_tmp.hrl").
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


%% Full 'initial_value' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries ('initial_value' must be
%% proper json object which implies binaries as keys). Instead it is shown below:
%%
%% #{
%%      <<"end">> := integer(),
%%      <<"start">> => integer(),  % default `0`
%%      <<"step">> => integer()    % default `1`
%% }
-type initial_value() :: #{binary() => integer()}.
-type operation_options() :: #{binary() => boolean()}.

-record(atm_range_store_container, {
    data_spec :: atm_data_spec:record(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_store_container{}.

-export_type([initial_value/0, operation_options/0, record/0]).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(atm_workflow_execution_ctx:record(), atm_data_spec:record(), initial_value()) ->
    record() | no_return().
create(AtmWorkflowExecutionCtx, AtmDataSpec, #{<<"end">> := EndNum} = InitialArgs) ->
    StartNum = maps:get(<<"start">>, InitialArgs, 0),
    Step = maps:get(<<"step">>, InitialArgs, 1),

    assert_supported_data_spec(AtmDataSpec),
    validate_range(AtmWorkflowExecutionCtx, AtmDataSpec, StartNum, EndNum, Step),

    #atm_range_store_container{
        data_spec = AtmDataSpec,
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    };
create(_AtmWorkflowExecutionCtx, _AtmDataSpec, _InitialArgs) ->
    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"end">>)).


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_range_store_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec browse_content(atm_workflow_execution_ctx:record(), atm_store_api:browse_opts(), record()) ->
    atm_store_api:browse_result() | no_return().
browse_content(_AtmWorkflowExecutionCtx, _Opts, #atm_range_store_container{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    Content = #{
        <<"start">> => StartNum,
        <<"end">> => EndNum,
        <<"step">> => Step
    },
    {[{<<>>, {ok, Content}}], true}.


-spec acquire_iterator(record()) -> atm_range_store_container_iterator:record().
acquire_iterator(#atm_range_store_container{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    atm_range_store_container_iterator:build(StartNum, EndNum, Step).


-spec apply_operation(record(), atm_store_container:operation()) -> no_return().
apply_operation(_Record, _AtmStoreContainerOperation) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(_Record) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_range_store_container{
    data_spec = AtmDataSpec,
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}, NestedRecordEncoder) ->
    #{
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"start">> => StartNum,
        <<"end">> => EndNum,
        <<"step">> => Step
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"dataSpec">> := AtmDataSpecJson,
    <<"start">> := StartNum,
    <<"end">> := EndNum,
    <<"step">> := Step
}, NestedRecordDecoder) ->
    #atm_range_store_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_supported_data_spec(atm_data_spec:record()) -> ok | no_return().
assert_supported_data_spec(AtmDataSpec) ->
    case atm_data_spec:get_type(AtmDataSpec) of
        atm_integer_type ->
            ok;
        AtmDataType ->
            throw(?ERROR_ATM_UNSUPPORTED_DATA_TYPE(AtmDataType, [atm_integer_type]))
    end.


%% @private
-spec validate_range(
    atm_workflow_execution_ctx:record(),
    atm_data_spec:record(),
    integer(), integer(), integer()
) ->
    ok | no_return().
validate_range(AtmWorkflowExecutionCtx, AtmDataSpec, StartNum, EndNum, Step) ->
    lists:foreach(fun({ArgName, ArgValue}) ->
        try
            atm_value:validate(AtmWorkflowExecutionCtx, ArgValue, AtmDataSpec)
        catch throw:Reason  ->
            throw(?ERROR_ATM_BAD_DATA(ArgName, Reason))
        end
    end, [
        {<<"start">>, StartNum},
        {<<"end">>, EndNum},
        {<<"step">>, Step}
    ]),
    assert_proper_range(StartNum, EndNum, Step).


%% @private
-spec assert_proper_range(integer(), integer(), integer()) -> ok | no_return().
assert_proper_range(Start, End, Step) when Start =< End, Step > 0 ->
    ok;
assert_proper_range(Start, End, Step) when Start >= End, Step < 0 ->
    ok;
assert_proper_range(_Start, _End, _Step) ->
    throw(?ERROR_ATM_BAD_DATA(<<"range">>, <<"invalid range specification">>)).
