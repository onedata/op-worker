%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides convinience functions designed for 
%% REST handling modules.
%% @end
%% ===================================================================

-module(rest_utils).

-include("err.hrl").

-export([map/2, unmap/3, encode_to_json/1, decode_from_json/1]).
-export([success_reply/1, error_reply/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% map/2
%% ====================================================================
%% @doc Converts a record to JSON conversion-ready tuple list.
%% For this input:
%% RecordToMap = #some_record{id=123, message="mess"}
%% Fields = [id, message]
%% The function will produce: [{id, 123},{message, "mess"}]
%% @end
-spec map(record(), [atom()]) -> [tuple()].
%% ====================================================================
map(RecordToMap, Fields) ->
    Y = [try N = lists:nth(1, B), if is_number(N) -> wf:to_binary(B); true -> B end catch _:_ -> B end
        || B <- tl(tuple_to_list(RecordToMap))],
    lists:zip(Fields, Y).


%% unmap/3
%% ====================================================================
%% @doc Converts a tuple list resulting from JSON to erlang translation
%% into an erlang record. Reverse process to map/2.
%% For this input: 
%% Proplist = [{id, 123},{message, "mess"}]
%% RecordTuple = #some_record{}
%% Fields = [id, message]
%% The function will produce: #some_record{id=123, message="mess"}
%% @end
-spec unmap([tuple()], record(), [atom()]) -> [tuple()].
%% ====================================================================
unmap([], RecordTuple, _) ->
    RecordTuple;

unmap([{KeyBin, Val} | Proplist], RecordTuple, Fields) ->
    Key = wf:to_atom(KeyBin),
    Value = case Val of
                I when is_integer(I) -> Val;
                A when is_atom(A) -> Val;
                _ -> gui_utils:to_list(Val)
            end,
    Index = string:str(Fields, [Key]) + 1,
    true = (Index > 1),
    unmap(Proplist, setelement(Index, RecordTuple, Value), Fields).


%% encode_to_json/1
%% ====================================================================
%% @doc Convinience function that convert an erlang term to JSON, producing
%% binary result.
%%
%% Possible terms, can be nested:
%% {struct, Props} - Props is a structure as a proplist, e.g.: [{id, 13}, {message, "mess"}]
%% {Props} - alias for above
%% {array, Array} - Array is a list, e.g.: [13, "mess"]
%% @end
-spec encode_to_json(term()) -> binary().
%% ====================================================================
encode_to_json(Term) ->
    iolist_to_binary(mochijson2:encode(Term)).


%% decode_from_json/1
%% ====================================================================
%% @doc Convinience function that convert JSON binary to an erlang term.
%% @end
-spec decode_from_json(term()) -> binary().
%% ====================================================================
decode_from_json(JSON) ->
    mochijson2:decode(JSON).


%% success_reply/1
%% ====================================================================
%% @doc Produces a standarized JSON return message, indicating success of an operation.
%% It can be inserted directly into response body. Macros from rest_messages.hrl should
%% be used as an argument to this function.
%% @end
-spec success_reply(binary()) -> binary().
%% ====================================================================
success_reply({Code, Message}) ->
    <<"{\"status\":\"ok\",\"code\":\"", Code/binary, "\",\"description\":\"", Message/binary, "\"}">>.


%% error_reply/1
%% ====================================================================
%% @doc Produces a standarized JSON return message, indicating failure of an operation.
%% It can be inserted directly into response body. #error_rec{} from err.hrl should
%% be used as an argument to this function.
%% @end
-spec error_reply(#error_rec{}) -> binary().
%% ====================================================================
error_reply(Record) ->
    rest_utils:encode_to_json({struct, rest_utils:map(Record, [status, code, description])}).