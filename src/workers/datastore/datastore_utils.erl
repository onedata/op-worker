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

-include("workers/datastore/datastore.hrl").

-define(KEY_LEN, 32).

%% API
-export([shallow_to_map/1, shallow_to_record/1, gen_uuid/0]).

%%%===================================================================
%%% API
%%%===================================================================

shallow_to_map(#{'$record' := ModelName} = Map) when is_atom(ModelName) ->
    Map;
shallow_to_map(Record) when is_tuple(Record) ->
    ModelName = element(1, Record),
    #model_config{fields = Fields} = ModelName:model_init(),
    [_ | Values1] = tuple_to_list(Record),
    Map = maps:from_list(lists:zip(Fields, Values1)),
    Map#{'$record' => ModelName}.


shallow_to_record(Record) when is_tuple(Record) ->
    Record;
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


%%%===================================================================
%%% Internal functions
%%%===================================================================