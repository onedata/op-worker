%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Utility and common functions for datastore module.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_utils).
-author("Rafal Slota").

-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").

-define(KEY_LEN, 32).

%% API
-export([shallow_to_map/1, shallow_to_record/1, gen_uuid/0, gen_uuid/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Converts given tuple or record into map.
%% @end
%%--------------------------------------------------------------------
-spec shallow_to_map(tuple()) -> #{term() => term()}.
shallow_to_map(#{'$record' := ModelName} = Map) when is_atom(ModelName) ->
    Map;
shallow_to_map(Record) when is_tuple(Record) ->
    ModelName = element(1, Record),
    try ModelName:model_init() of
        #model_config{fields = Fields} ->
            [_ | Values1] = tuple_to_list(Record),
            Map = maps:from_list(lists:zip(Fields, Values1)),
            Map#{'$record' => ModelName}
    catch
        _:_ -> %% encode as tuple
            Values = tuple_to_list(Record),
            Keys = lists:seq(1, length(Values)),
            KeyValue = lists:zip(Keys, Values),
            Map = maps:from_list(KeyValue),
            Map#{'$record' => undefined}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Converts given map to record or tuple (reverses shallow_to_map/1).
%% @end
%%--------------------------------------------------------------------
-spec shallow_to_record(tuple() | #{term() => term()}) -> tuple().
shallow_to_record(Record) when is_tuple(Record) ->
    Record;
shallow_to_record(#{'$record' := undefined} = Map) ->
    MapList = maps:to_list(maps:remove('$record', Map)),
    FieldList = [FieldValue || {_, FieldValue} <- lists:usort(MapList)],
    list_to_tuple(FieldList);
shallow_to_record(#{'$record' := ModelName} = Map) ->

    #model_config{fields = Fields, defaults = Defaults} = ModelName:model_init(),
    [_ | Values1] = tuple_to_list(Defaults),
    Defaults1 = lists:zip(Fields, Values1),
    Values =
        lists:map(
            fun({FieldName, DefaultValue}) ->
                maps:get(FieldName, Map, DefaultValue)
            end, Defaults1),

    list_to_tuple([ModelName | Values]).


%%--------------------------------------------------------------------
%% @doc
%% Generates random UUID.
%% @end
%%--------------------------------------------------------------------
-spec gen_uuid() -> binary().
gen_uuid() ->
    base64:encode(crypto:rand_bytes(?KEY_LEN)).


%%--------------------------------------------------------------------
%% @doc
%% Generates UUID based on given term.
%% @end
%%--------------------------------------------------------------------
-spec gen_uuid(term()) -> binary().
gen_uuid(Term) ->
    base64:encode(term_to_binary(Term)).


%%%===================================================================
%%% Internal functions
%%%===================================================================