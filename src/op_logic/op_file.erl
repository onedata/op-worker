%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_file model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_file).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2]).
-export([authorize/2, data_signature/1]).

-define(DEFAULT_LIST_OFFSET, 0).
-define(DEFAULT_LIST_ENTRIES, 1000).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_file.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, attributes, private) -> true;
operation_supported(create, metadata, private) -> true;

operation_supported(get, list, private) -> true;
operation_supported(get, attributes, private) -> true;
operation_supported(get, metadata, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = attributes}}) ->
    ok;

create(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = metadata}}) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, data = Data, gri = #gri{id = FileGuid, aspect = list}}, _) ->
    SessionId = Cl#client.id,
    Limit = maps:get(<<"limit">>, Data, ?DEFAULT_LIST_ENTRIES),
    Offset = maps:get(<<"offset">>, Data, ?DEFAULT_LIST_OFFSET),

    {ok, Path} = logical_file_manager:get_file_path(SessionId, FileGuid),

    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE, guid = Guid}} ->
            {ok, Children} = logical_file_manager:ls(SessionId, {guid, FileGuid}, Offset, Limit),
            {ok, lists:map(fun({ChildGuid, ChildPath}) ->
                    {ok, ObjectId} = file_id:guid_to_objectid(ChildGuid),
                    #{<<"id">> => ObjectId, <<"path">> => filename:join(Path, ChildPath)}
            end, Children)};
        {ok, #file_attr{guid = Guid}} ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            {ok, [#{<<"id">> => ObjectId, <<"path">> => Path}]};
        Error ->
            Error
   end;

get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = attributes}}, _) ->
    ok;

get(#op_req{client = Cl, gri = #gri{id = FileGuid, aspect = metadata}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(#op_req{operation = create, gri = #gri{aspect = attribute}}) -> #{
    optional => #{<<"extended">> => {boolean, any}}
};

data_signature(#op_req{operation = create, gri = #gri{aspect = metadata}}) -> #{
    optional => #{
        <<"metadata_type">> => {binary, [<<"JSON">>, <<"RDF">>]},
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = list}}) -> #{
    optional => #{
        <<"limit">> => {integer, {between, 1, 1000}},
        <<"offset">> => {integer, {not_lower_than, 0}}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = attribute}}) -> #{
    optional => #{
        <<"attribute">> => {binary, any},
        <<"extended">> => {boolean, any},
        <<"inherited">> => {boolean, any}
    }
};

data_signature(#op_req{operation = get, gri = #gri{aspect = metadata}}) -> #{
    optional => #{
        <<"metadata_type">> => {binary, [<<"JSON">>, <<"RDF">>]},
        <<"filter_type">> => {binary, any},
        <<"filter">> => {binary, any},
        <<"inherited">> => {boolean, any}
    }
};

data_signature(_) -> #{}.
