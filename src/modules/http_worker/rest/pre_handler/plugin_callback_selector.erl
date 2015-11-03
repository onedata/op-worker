%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions that select user defined handlers registered in
%%% content_types_provided and content_types_accepted functions.
%%% @end
%%%--------------------------------------------------------------------
-module(plugin_callback_selector).
-author("Tomasz Lichon").

-type content_type_callback() ::
{binary() | {Type :: binary(), SubType :: binary(), content_type_params()}, module()}.

-type content_type_params() :: '*' | [{binary(), binary()}].

-include("modules/http_worker/rest/http_status.hrl").

%% API
-export([select_accept_callback/1, select_provide_callback/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Select callback that accepts incoming resource
%% @end
%%--------------------------------------------------------------------
-spec select_accept_callback(cowboy_req:req()) -> {ok, {cowboy_req:req(), atom()}}.
select_accept_callback(Req) ->
    ContentTypesAccepted = request_context:get_content_types_accepted(),
    NormalizedContentTypesAccepted = normalize_content_types(ContentTypesAccepted),
    {ok, ContentType, Req2} = cowboy_req:parse_header(<<"content-type">>, Req),
    {ok, {Req2, choose_content_type_callback(ContentType, NormalizedContentTypesAccepted)}}.

%%--------------------------------------------------------------------
%% @doc
%% Select callback that provides resource.
%% @end
%%--------------------------------------------------------------------
-spec select_provide_callback(cowboy_req:req()) -> {ok, {cowboy_req:req(), atom()}}.
select_provide_callback(Req) ->
    ContentTypesProvided = request_context:get_content_types_provided(),
    NormalizedContentTypesProvided = normalize_content_types(ContentTypesProvided),
    case cowboy_req:parse_header(<<"accept">>, Req) of
        {ok, undefined, Req2} ->
            % todo check language (as in cowboy)
            [{_, Fun} | _] = NormalizedContentTypesProvided,
            {ok, {Req2, Fun}};
        {ok, ContentType, Req2} ->
            {ok, {Req2, choose_content_type_callback(ContentType, NormalizedContentTypesProvided)}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Normalizes content_type_callback() list to form
%% {Type :: binary(), SubType :: binary(), content_type_params()}
%% @end
%%--------------------------------------------------------------------
-spec normalize_content_types([content_type_callback()]) -> [content_type_callback()].
normalize_content_types([]) ->
    [];
normalize_content_types([{ContentType, Callback} | Rest]) when is_binary(ContentType) ->
    [{cowboy_http:content_type(ContentType), Callback} | normalize_content_types(Rest)];
normalize_content_types([Normalized | Rest]) ->
    [Normalized | normalize_content_types(Rest)].

%%--------------------------------------------------------------------
%% @doc
%% Chooses callback that can handle given content type.
%% @end
%%--------------------------------------------------------------------
-spec choose_content_type_callback(binary(), [content_type_callback()]) -> atom().
choose_content_type_callback(ContentType, [{Accepted, Fun} | _Tail])
    when Accepted =:= '*'; Accepted =:= ContentType ->
    Fun;
choose_content_type_callback({Type, SubType, Param}, [{{Type, SubType, AcceptedParam}, Fun} | _Tail])
    when AcceptedParam =:= '*'; AcceptedParam =:= Param ->
    Fun;
choose_content_type_callback(ContentType, [_ | Tail]) ->
    choose_content_type_callback(ContentType, Tail).
