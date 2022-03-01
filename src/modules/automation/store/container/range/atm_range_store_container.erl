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

-include("modules/automation/atm_execution.hrl").

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


%% Full 'initial_content' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries ('initial_content' must be
%% proper json object which implies binaries as keys). Instead it is shown below:
%%
%% #{
%%      <<"end">> := integer(),
%%      <<"start">> => integer(),  % default `0`
%%      <<"step">> => integer()    % default `1`
%% }
-type initial_content() :: #{binary() => integer()}.

-type content_browse_req() :: #atm_store_content_browse_req{
    options :: atm_range_store_content_browse_options:record()
}.
-type content_update_req() :: #atm_store_content_update_req{
    options :: atm_range_store_content_update_options:record()
}.

-record(atm_range_store_container, {
    config :: atm_range_store_config:record(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type record() :: #atm_range_store_container{}.

-export_type([
    initial_content/0, content_browse_req/0, content_update_req/0,
    record/0
]).


-define(ITEM_DATA_SPEC, #atm_data_spec{type = atm_integer_type}).


%%%===================================================================
%%% atm_store_container callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_auth:record(),
    atm_range_store_config:record(),
    initial_content()
) ->
    record() | no_return().
create(AtmWorkflowExecutionAuth, AtmStoreConfig, #{<<"end">> := EndNum} = InitialContent) ->
    StartNum = maps:get(<<"start">>, InitialContent, 0),
    Step = maps:get(<<"step">>, InitialContent, 1),

    validate_range(AtmWorkflowExecutionAuth, StartNum, EndNum, Step),

    #atm_range_store_container{
        config = AtmStoreConfig,
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    };
create(_AtmWorkflowExecutionAuth, _AtmStoreConfig, _InitialContent) ->
    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"end">>)).


-spec get_config(record()) -> atm_range_store_config:record().
get_config(#atm_range_store_container{config = AtmStoreConfig}) ->
    AtmStoreConfig.


-spec get_iterated_item_data_spec(record()) -> atm_data_spec:record().
get_iterated_item_data_spec(_) ->
    ?ITEM_DATA_SPEC.


-spec acquire_iterator(record()) -> atm_range_store_container_iterator:record().
acquire_iterator(#atm_range_store_container{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    atm_range_store_container_iterator:build(StartNum, EndNum, Step).


-spec browse_content(record(), content_browse_req()) ->
    atm_store_api:browse_result() | no_return().
browse_content(
    #atm_range_store_container{start_num = StartNum, end_num = EndNum, step = Step},
    #atm_store_content_browse_req{options = #atm_range_store_content_browse_options{}}
) ->
    Content = #{<<"start">> => StartNum, <<"end">> => EndNum, <<"step">> => Step},
    {[{<<>>, {ok, Content}}], true}.


-spec update_content(record(), content_update_req()) -> no_return().
update_content(_Record, _UpdateAtmStoreContainerContent) ->
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
    config = AtmStoreConfig,
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}, NestedRecordEncoder) ->
    #{
        <<"config">> => NestedRecordEncoder(AtmStoreConfig, atm_range_store_config),
        <<"start">> => StartNum,
        <<"end">> => EndNum,
        <<"step">> => Step
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"config">> := AtmStoreConfigJson,
    <<"start">> := StartNum,
    <<"end">> := EndNum,
    <<"step">> := Step
}, NestedRecordDecoder) ->
    #atm_range_store_container{
        config = NestedRecordDecoder(AtmStoreConfigJson, atm_range_store_config),
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec validate_range(atm_workflow_execution_auth:record(), integer(), integer(), integer()) ->
    ok | no_return().
validate_range(AtmWorkflowExecutionAuth, StartNum, EndNum, Step) ->
    lists:foreach(fun({ArgName, ArgValue}) ->
        try
            atm_value:validate(AtmWorkflowExecutionAuth, ArgValue, ?ITEM_DATA_SPEC)
        catch Type:Reason:Stacktrace ->
            Error = ?atm_examine_error(Type, Reason, Stacktrace),
            throw(?ERROR_BAD_DATA(ArgName, Error))
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
    throw(?ERROR_BAD_DATA(<<"range">>, <<"invalid range specification">>)).
