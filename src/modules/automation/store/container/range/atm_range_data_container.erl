%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_container` functionality for
%%% `range` atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_data_container).
-author("Bartosz Walkowicz").

-behaviour(atm_data_container).

-include_lib("ctool/include/errors.hrl").

%% atm_data_container callbacks
-export([
    init/2,
    get_data_spec/1,
    get_data_stream/1,
    to_json/1,
    from_json/1
]).

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

-record(atm_range_data_container, {
    data_spec :: atm_data_spec:spec(),
    start_num :: integer(),
    end_num :: integer(),
    step :: integer()
}).
-type container() :: #atm_range_data_container{}.

-export_type([init_args/0, container/0]).


%%%===================================================================
%%% atm_data_container callbacks
%%%===================================================================


-spec init(atm_data_spec:spec(), init_args()) -> container() | no_return().
init(AtmDataSpec, #{<<"end">> := EndNum} = InitialArgs) when is_integer(EndNum) ->
    StartNum = maps:get(<<"start">>, InitialArgs, 0),
    Step = maps:get(<<"step">>, InitialArgs, 1),

    case
        atm_data_spec:get_type(AtmDataSpec) =:= atm_integer_type andalso
        is_integer(StartNum) andalso is_integer(Step) andalso
        is_proper_range(StartNum, EndNum, Step)
    of
        true ->
            #atm_range_data_container{
                data_spec = AtmDataSpec,
                start_num = StartNum,
                end_num = EndNum,
                step = Step
            };
        false ->
            throw(?EINVAL)
    end;
init(_AtmDataSpec, _InitialArgs) ->
    throw(?EINVAL).


%% @private
-spec is_proper_range(integer(), integer(), integer()) -> boolean().
is_proper_range(Start, End, Step) when Start =< End, Step > 0 ->
    true;
is_proper_range(Start, End, Step) when Start >= End, Step < 0 ->
    true;
is_proper_range(_Start, _End, _Step) ->
    false.


-spec get_data_spec(container()) -> atm_data_spec:spec().
get_data_spec(#atm_range_data_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec get_data_stream(container()) -> atm_range_data_stream:stream().
get_data_stream(#atm_range_data_container{
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    atm_range_data_stream:init(StartNum, EndNum, Step).


-spec to_json(container()) -> json_utils:json_map().
to_json(#atm_range_data_container{
    data_spec = AtmDataSpec,
    start_num = StartNum,
    end_num = EndNum,
    step = Step
}) ->
    #{
        <<"dataSpec">> => atm_data_spec:to_json(AtmDataSpec),
        <<"start">> => StartNum,
        <<"end">> => EndNum,
        <<"step">> => Step
    }.


-spec from_json(json_utils:json_map()) -> container().
from_json(#{
    <<"dataSpec">> := AtmDataSpecJson,
    <<"start">> := StartNum,
    <<"end">> := EndNum,
    <<"step">> := Step
}) ->
    #atm_range_data_container{
        data_spec = atm_data_spec:from_json(AtmDataSpecJson),
        start_num = StartNum,
        end_num = EndNum,
        step = Step
    }.
