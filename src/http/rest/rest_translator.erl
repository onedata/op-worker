%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-5621
%%% This module handles translation of request results to REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(rest_translator).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

%% API
-export([response/2, error_response/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec response(_, middleware:result()) -> #rest_resp{}.
response(_, {error, _} = Error) ->
    error_response(Error);

response(#op_req{operation = create}, ok) ->
    % No need for translation, 'ok' means success with no response data
    ?NO_CONTENT_REPLY;
response(#op_req{operation = create} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}, auth_hint = AuthHint} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:create_response(GRI, AuthHint, DataFormat, Result);

response(#op_req{operation = get} = OpReq, {ok, Data}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:get_response(GRI, Data);
response(#op_req{operation = get} = OpReq, {ok, value, Data}) ->
    response(OpReq, {ok, Data});

response(#op_req{operation = update}, ok) ->
    ?NO_CONTENT_REPLY;

response(#op_req{operation = delete}, ok) ->
    ?NO_CONTENT_REPLY;
response(#op_req{operation = delete} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:delete_response(GRI, DataFormat, Result).


-spec error_response(errors:error()) -> #rest_resp{}.
error_response({error, _} = Error) ->
    #rest_resp{
        code = errors:to_http_code(Error),
        headers = #{<<"content-type">> => <<"application/json">>},
        body = #{<<"error">> => errors:to_json(Error)}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec entity_type_to_translator(atom()) -> module().
entity_type_to_translator(op_file) -> file_rest_translator;
entity_type_to_translator(op_metrics) -> metrics_rest_translator;
entity_type_to_translator(op_provider) -> provider_rest_translator;
entity_type_to_translator(op_qos) -> qos_rest_translator;
entity_type_to_translator(op_replica) -> replica_rest_translator;
entity_type_to_translator(op_share) -> share_rest_translator;
entity_type_to_translator(op_space) -> space_rest_translator;
entity_type_to_translator(op_transfer) -> transfer_rest_translator.
