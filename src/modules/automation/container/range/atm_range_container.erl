%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `range`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_container).
-author("Bartosz Walkowicz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/2, get_data_spec/1, get_container_stream/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


%% Full 'init_args' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"end">> := integer(),
%%      <<"start">> => integer(),  % default `0`
%%      <<"step">> => integer()    % default `1`
%% }
-type init_args() :: map().

-record(atm_range_container, {
    data_spec :: atm_data_spec:record(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type container() :: #atm_range_container{}.

-export_type([init_args/0, container/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), init_args()) -> container() | no_return().
create(AtmDataSpec, #{<<"end">> := EndNum} = InitialArgs) ->
    StartNum = maps:get(<<"start">>, InitialArgs, 0),
    Step = maps:get(<<"step">>, InitialArgs, 1),

    validate_init_args(StartNum, EndNum, Step, AtmDataSpec),

    #atm_range_container{
        data_spec = AtmDataSpec,
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    };
create(_AtmDataSpec, _InitialArgs) ->
    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"end">>)).


-spec get_data_spec(container()) -> atm_data_spec:record().
get_data_spec(#atm_range_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec get_container_stream(container()) -> atm_range_container_stream:stream().
get_container_stream(#atm_range_container{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    atm_range_container_stream:create(StartNum, EndNum, Step).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(container(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_range_container{
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
    container().
db_decode(#{
    <<"dataSpec">> := AtmDataSpecJson,
    <<"start">> := StartNum,
    <<"end">> := EndNum,
    <<"step">> := Step
}, NestedRecordDecoder) ->
    #atm_range_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec validate_init_args(integer(), integer(), integer(), atm_data_spec:record()) ->
    ok | no_return().
validate_init_args(StartNum, EndNum, Step, AtmDataSpec) ->
    %% TODO more descriptive logs
    case
        atm_data_spec:get_type(AtmDataSpec) =:= atm_integer_type andalso
        is_integer(StartNum) andalso is_integer(EndNum) andalso is_integer(Step) andalso
        is_proper_range(StartNum, EndNum, Step)
    of
        true ->
            ok;
        false ->
            throw(?EINVAL)
    end.


%% @private
-spec is_proper_range(integer(), integer(), integer()) -> boolean().
is_proper_range(Start, End, Step) when Start =< End, Step > 0 ->
    true;
is_proper_range(Start, End, Step) when Start >= End, Step < 0 ->
    true;
is_proper_range(_Start, _End, _Step) ->
    false.
