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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([response/2, error_response/1]).


%%%===================================================================
%%% API
%%%===================================================================


-spec response(middleware:req(), middleware:result()) -> #rest_resp{}.
response(#op_req{operation = Operation, gri = GRI} = OpReq, Result) ->
    try
        response_insecure(OpReq, Result)
    catch Type:Message ->
        ?error_stacktrace("Cannot translate REST result for:~n"
                          "Operation: ~p~n"
                          "GRI: ~p~n"
                          "Result: ~p~n"
                          "---------~n"
                          "Error was: ~w:~p", [
            Operation, GRI, Result, Type, Message
        ]),
        error_response(?ERROR_INTERNAL_SERVER_ERROR)
    end.


-spec error_response(errors:error()) -> #rest_resp{}.
error_response({error, _} = Error) ->
    #rest_resp{
        code = errors:to_http_code(Error),
        headers = #{?HDR_CONTENT_TYPE => <<"application/json">>},
        body = #{<<"error">> => errors:to_json(Error)}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec response_insecure(middleware:req(), middleware:result()) -> #rest_resp{}.
response_insecure(#op_req{operation = create}, ok) ->
    % No need for translation, 'ok' means success with no response data
    ?NO_CONTENT_REPLY;
response_insecure(#op_req{operation = create} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}, auth_hint = AuthHint} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:create_response(GRI, AuthHint, DataFormat, Result);

response_insecure(#op_req{operation = get} = OpReq, {ok, Data}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:get_response(GRI, Data);
response_insecure(#op_req{operation = get} = OpReq, {ok, value, Data}) ->
    response_insecure(OpReq, {ok, Data});

response_insecure(#op_req{operation = update}, ok) ->
    ?NO_CONTENT_REPLY;

response_insecure(#op_req{operation = delete}, ok) ->
    ?NO_CONTENT_REPLY;
response_insecure(#op_req{operation = delete} = OpReq, {ok, DataFormat, Result}) ->
    #op_req{gri = GRI = #gri{type = Model}} = OpReq,
    Translator = entity_type_to_translator(Model),
    Translator:delete_response(GRI, DataFormat, Result);

response_insecure(_, {error, _} = Error) ->
    error_response(Error).


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
